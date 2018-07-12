package softcache

import (
	"errors"
	"strings"
	"time"
	"github.com/101medialab/go-redis-distributed-lock"
	"github.com/go-redis/redis"
	"github.com/patrickmn/go-cache"
)

const lockDuration = time.Minute

var pManagerLockOptions = &redisLock.LockOptions{
	lockDuration,
	lockDuration,
}

var ErrWaitTooLong = errors.New(`SoftCache - The transaction is not proceeded due to long waiting. `)

type CacheManager struct {
	RedisClient                  *redis.Client          `inject:""`
	LockFactory                  *redisLock.LockFactory `inject:""`
	recentRegistrations          *cache.Cache
	cacheRefreshers              map[string]CacheRefresherOptions
	taskChannel                  chan string
	cacheRefreshingTasksListName string
	cacheRefreshingTasksLockName string
	cacheRefreshingLockPrefix    string
}

func New(
	cacheRefreshingLockPrefix string,
	listLockName string,
	listName string,
) *CacheManager {
	return &CacheManager{
		recentRegistrations:          cache.New(lockDuration, lockDuration),
		cacheRefreshers:              map[string]CacheRefresherOptions{},
		taskChannel:                  make(chan string),
		cacheRefreshingTasksListName: listName,
		cacheRefreshingTasksLockName: listLockName,
		cacheRefreshingLockPrefix:    cacheRefreshingLockPrefix,
	}
}

func (cm *CacheManager) Start() {
	go cm.rebuildCacheFromTaskChannel()
	go cm.addTaskToChannelIfSoftTTLReached()
}

func (cm *CacheManager) AddCacheRefresher(options CacheRefresherOptions) error {
	name := options.Callback.GetName()

	if _, ok := cm.cacheRefreshers[name]; ok {
		errors.New(`SoftCache - Cache Refresher has been added. Name: ` + name)
	}

	cm.cacheRefreshers[name] = options

	return nil
}

func getCacheId(refresherID, input string) string {
	return refresherID + `-` + input
}

func getRefresherIDAndInput(cacheId string) (refresherID string, input string) {
	temp := strings.SplitN(cacheId, `-`, 2)
	return temp[0], temp[1]
}

func (cm *CacheManager) GetManagerLock() *redisLock.Lock {
	return cm.LockFactory.Lock(
		cm.cacheRefreshingTasksLockName,
		pManagerLockOptions,
	)
}

func (cm *CacheManager) Refresh(refresherID, inputJSON string, isForceReload bool) error {
	cacheRefresher, ok := cm.cacheRefreshers[refresherID]
	if !ok {
		return errors.New(`SoftCache - Refreshing unrecognized cache. refresherID: ` + refresherID)
	}

	cacheId := getCacheId(refresherID, inputJSON)
	if isForceReload {
		if err := cm.RedisClient.Del(cacheId).Err(); err != nil {
			return err
		}
	} else {
		// Further reduce TTL by 10 seconds
		if ttl := cacheRefresher.HardTtl - cacheRefresher.SoftTtl - 10*time.Second; ttl > 0 {
			if err := cm.RedisClient.Expire(cacheId, ttl).Err(); err != nil {
				return err
			}
		}
	}

	lock := cm.GetManagerLock()

	if lock != nil {
		defer lock.Release()
	}

	return cm.RedisClient.ZAdd(
		cm.cacheRefreshingTasksListName,
		redis.Z{
			Score:  float64(time.Now().Unix()),
			Member: cacheId,
		},
	).
		Err()
}

func (cm *CacheManager) GetData(refresherID string, input string) (string, error) {
	cacheRefresher, ok := cm.cacheRefreshers[refresherID]
	if !ok {
		return ``, errors.New(`This resultJSON set has not added yet. refresherID: ` + refresherID)
	}

	cacheId := getCacheId(refresherID, input)

	if resultJSON, err := cm.RedisClient.Get(cacheId).Result(); err == nil && resultJSON != `` {
		go cm.registerTaskToRefreshList(cacheId, cacheRefresher)

		return resultJSON, nil
	} else {
		if err != redis.Nil {
			return ``, err
		}
	}

	lock := cm.LockFactory.Lock(
		cm.cacheRefreshingLockPrefix+`-`+cacheId,
		cacheRefresher.TaskSetting,
	)
	if lock == nil {
		return ``, ErrWaitTooLong
	}
	defer lock.Release()

	// TBH, it is quite unlikely you will find cache but yeah....
	// Try to get cache again, as it is possible cache has been refreshed just now
	if resultJSON, err := cm.RedisClient.Get(cacheId).Result(); err == nil && resultJSON != `` {
		return resultJSON, nil
	} else {
		if err != redis.Nil {
			return ``, err
		}
	}

	resultJSON, err := cacheRefresher.Callback.Refresh(input)
	if err == nil {
		cm.RedisClient.Set(cacheId, resultJSON, cacheRefresher.HardTtl)
	}

	return resultJSON, nil
}

func (cm *CacheManager) registerTaskToRefreshList(cacheId string, cacheRefresherOptions CacheRefresherOptions) {
	if _, isRegistered := cm.recentRegistrations.Get(cacheId); isRegistered {
		return
	}

	ttlFromRedis, err0 := cm.RedisClient.TTL(cacheId).Result()
	if err0 != nil {
		time.Sleep(time.Second * 30)

		println(`CacheManager::rebuildCacheFromTaskChannel failed: ` + err0.Error())
	}

	softTTL := cacheRefresherOptions.SoftTtl - (cacheRefresherOptions.HardTtl - ttlFromRedis) +
	// To avoid time inconsistency between servers
		5*time.Second

	// when the refresh worker pick up this event, it must be passed the SoftTtl
	score := time.Now().Add(softTTL).Unix()

	if softTTL > 0 {
		cm.recentRegistrations.Add(cacheId, "DONOTCARE", softTTL)
	}

	lock := cm.LockFactory.Lock(
		cm.cacheRefreshingTasksLockName,
		pManagerLockOptions,
	)

	if lock != nil {
		defer lock.Release()
	}

	cm.RedisClient.ZAdd(
		cm.cacheRefreshingTasksListName,
		redis.Z{
			Score:  float64(score),
			Member: cacheId,
		},
	)
}

func (cm *CacheManager) addTaskToChannelIfSoftTTLReached() {
	for {
		if len(cm.taskChannel) == 0 {
			lock := cm.LockFactory.Lock(
				cm.cacheRefreshingTasksLockName,
				pManagerLockOptions,
			)

			if lock == nil {
				return
			}

			timeNow := float64(time.Now().Unix())

			candidates, err1 := cm.RedisClient.ZRangeWithScores(cm.cacheRefreshingTasksListName, 0, 10).Result()
			if err1 != nil {
				time.Sleep(time.Second * 30)

				println(`CacheManager::rebuildCacheFromTaskChannel failed: ` + err1.Error())
			}

			for _, cacheIdAndInput := range candidates {
				if cacheIdAndInput.Score > timeNow {
					break
				}
				// remove the current record from the set
				cacheId := cacheIdAndInput.Member.(string)
				cm.RedisClient.ZRem(cm.cacheRefreshingTasksListName, cacheId)

				// and then pass to the worker
				cm.taskChannel <- cacheId
			}

			lock.Release()
		}

		time.Sleep(5 * time.Second)
	}
}

func (cm *CacheManager) rebuildCacheFromTaskChannel() {
	for {
		cacheId := <-cm.taskChannel
		refresherID, inputJSON := getRefresherIDAndInput(cacheId)

		cacheRefresher, ok := cm.cacheRefreshers[refresherID]
		if !ok {
			continue
		}

		ttlFromRedis, err0 := cm.RedisClient.TTL(cacheId).Result()
		if err0 != nil {
			time.Sleep(time.Second * 30)

			println(`CacheManager::rebuildCacheFromTaskChannel failed: ` + err0.Error())
		}

		if cacheRefresher.IsStillWithinSoftTTL(ttlFromRedis) {
			continue
		}

		if resultJSON, err := cacheRefresher.Callback.Refresh(inputJSON); err == nil {
			cm.RedisClient.Set(cacheId, resultJSON, cacheRefresher.HardTtl)
		}
	}
}

type CacheRefresherOptions struct {
	Callback    CacheRefreshCallback
	TaskSetting *redisLock.LockOptions
	HardTtl     time.Duration // TTL set in Redis
	SoftTtl     time.Duration // TTL for SoftCache to refresh cache
}

func (rs CacheRefresherOptions) IsStillWithinSoftTTL(ttl time.Duration) bool {
	return rs.HardTtl-ttl < rs.SoftTtl
}

type CacheRefreshCallback interface {
	Refresh(input string) (string, error)
	GetName() string
}
