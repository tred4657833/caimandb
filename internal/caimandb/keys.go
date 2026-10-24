package caimandb

import (
	"fmt"
)

func docKey(block, id string) []byte {
	return []byte(prefixDoc + block + "/" + id)
}

func docKeyPrefix(block string) []byte {
	return []byte(prefixDoc + block + "/")
}

func metaKey(block string) []byte {
	return []byte(prefixMeta + block)
}

func secondaryIndexKey(block, field, value, id string) []byte {
	return []byte(fmt.Sprintf("%s%s/%s/%s/%s", prefixSecondary, block, field, value, id))
}

func secondaryIndexPrefix(block, field, value string) []byte {
	if value == "" {
		return []byte(fmt.Sprintf("%s%s/%s/", prefixSecondary, block, field))
	}
	return []byte(fmt.Sprintf("%s%s/%s/%s/", prefixSecondary, block, field, value))
}

func (e *Engine) cacheKey(dbName, block, id string) string {
	return dbName + "/" + block + "/" + id
}
