/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_CONTINUOUS_AGGS_CAGG_CREATE_H
#define TIMESCALEDB_TSL_CONTINUOUS_AGGS_CAGG_CREATE_H
#include <postgres.h>
#include <catalog/objectaddress.h>
#include <nodes/parsenodes.h>

#include "with_clause_parser.h"

#define CONTINUOUS_AGG_CHUNK_ID_COL_NAME "chunk_id"

typedef struct CAggTimebucketInfo
{
	int32 htid;				/* hypertable id */
	Oid htoid;				/* hypertable oid */
	AttrNumber htpartcolno; /*primary partitioning column */
							/* This should also be the column used by time_bucket */
	Oid htpartcoltype;
	int64 htpartcol_interval_len; /* interval length setting for primary partitioning column */
	int64 bucket_width;			  /*bucket_width of time_bucket */
} CAggTimebucketInfo;

typedef struct FinalizeQueryInfo
{
	List *final_seltlist;   /*select target list for finalize query */
	Node *final_havingqual; /*having qual for finalize query */
	Query *final_userquery; /* user query used to compute the finalize_query */
} FinalizeQueryInfo;

typedef struct MatTableColumnInfo
{
	List *matcollist;		 /* column defns for materialization tbl*/
	List *partial_seltlist;  /* tlist entries for populating the materialization table columns */
	List *partial_grouplist; /* group clauses used for populating the materialization table */
	List *mat_groupcolname_list; /* names of columns that are populated by the group-by clause
									correspond to the partial_grouplist.
									time_bucket column is not included here: it is the
									matpartcolname */
	int matpartcolno;			 /*index of partitioning column in matcollist */
	char *matpartcolname;		 /*name of the partition column */
} MatTableColumnInfo;

bool tsl_process_continuous_agg_viewstmt(ViewStmt *stmt, const char *query_string, void *pstmt,
										 WithClauseResult *with_clause_options);

Query *cagg_build_union_query(CAggTimebucketInfo *tbinfo, MatTableColumnInfo *mattblinfo, Query *q1,
							  Query *q2);
CAggTimebucketInfo cagg_validate_query(Query *query);
void finalizequery_init(FinalizeQueryInfo *inp, Query *orig_query, MatTableColumnInfo *mattblinfo);
void mattablecolumninfo_init(MatTableColumnInfo *matcolinfo, List *collist, List *tlist,
							 List *grouplist);
Query *finalizequery_get_select_query(FinalizeQueryInfo *inp, List *matcollist,
									  ObjectAddress *mattbladdress);

#endif /* TIMESCALEDB_TSL_CONTINUOUS_AGGS_CAGG_CREATE_H */
