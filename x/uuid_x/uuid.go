package uuid_x

import (
	"github.com/gocql/gocql"
)

func GetTimeUuid() *gocql.UUID {
	validUuid := gocql.TimeUUID()
	return &validUuid
}

func GetUuid(checkUuid string) *gocql.UUID {
	// Since this UUID probably we expect to be nil
	if checkUuid == "00000000-0000-0000-0000-000000000000" {
		return nil
	}
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
	validUuidStr := validUuid.String()
	if validUuidStr == "" || validUuidStr == "00000000-0000-0000-0000-000000000000" {
		return true
	}
	return false
}
