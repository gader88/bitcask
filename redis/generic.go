package redis

func (rds *RedisDataStructure) Del(key []byte) error {
	return rds.db.Delete(key)
}

func (rds *RedisDataStructure) Type(key []byte) (redisDataType, error) {
	encValue, err := rds.db.Get(key)
	if err != nil {
		return 0, err

	}
	if len(encValue) == 0 {
		return 0, nil
	}
	return redisDataType(encValue[0]), nil
}
