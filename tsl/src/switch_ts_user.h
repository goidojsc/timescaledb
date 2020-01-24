/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_SWITCH_TS_USER_H
#define TIMESCALEDB_TSL_SWITCH_TS_USER_H
#include <postgres.h>
#include <miscadmin.h>

#include "catalog.h"
#include "extension_constants.h"

/*switch to ts user for _timescaledb_internal access */
#define SWITCH_TO_TS_USER(schemaname, newuid, saved_uid, saved_secctx)                             \
	do                                                                                             \
	{                                                                                              \
		if (schemaname &&                                                                          \
			strncmp(schemaname, INTERNAL_SCHEMA_NAME, strlen(INTERNAL_SCHEMA_NAME)) == 0)          \
			newuid = ts_catalog_database_info_get()->owner_uid;                                    \
		else                                                                                       \
			newuid = InvalidOid;                                                                   \
		if (newuid != InvalidOid)                                                                  \
		{                                                                                          \
			GetUserIdAndSecContext(&saved_uid, &saved_secctx);                                     \
			SetUserIdAndSecContext(uid, saved_secctx | SECURITY_LOCAL_USERID_CHANGE);              \
		}                                                                                          \
	} while (0)

#define RESTORE_USER(newuid, saved_uid, saved_secctx)                                              \
	do                                                                                             \
	{                                                                                              \
		if (newuid != InvalidOid)                                                                  \
			SetUserIdAndSecContext(saved_uid, saved_secctx);                                       \
	} while (0);

#endif /* TIMESCALEDB_TSL_SWITCH_TS_USER_H */
