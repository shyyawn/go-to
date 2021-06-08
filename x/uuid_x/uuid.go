package uuid_x

import (
	"github.com/gocql/gocql"
)

func GetTimeUuid() *gocql.UUID {
	validUuid := gocql.TimeUUID()
	return &validUuid
}

func GetUuid(checkUuid string) *gocql.UUID {
	validUuid, err := gocql.ParseUUID(checkUuid)
	if err != nil {
		return nil
	}
	return &validUuid
}

func IsEmptyUuid(validUuid *gocql.UUID) bool {

	if validUuid == nil {
		return true
	}

	if validUuid.String() == "" {
		return true
	}
	return false
}
