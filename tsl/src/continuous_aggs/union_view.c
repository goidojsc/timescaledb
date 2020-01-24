/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <catalog/pg_type.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/parsenodes.h>
#include <nodes/primnodes.h>
#include <optimizer/clauses.h>
#include <parser/parse_func.h>
#include <utils/builtins.h>
#include <utils/typcache.h>

#include "continuous_aggs/create.h"
#include "extension_constants.h"

#define BOUNDARY_FUNCTION "cagg_watermark"
#define INTERNAL_TO_DATE_FUNCTION "to_date"
#define INTERNAL_TO_TSTZ_FUNCTION "to_timestamp"
#define INTERNAL_TO_TS_FUNCTION "to_timestamp_without_timezone"

static Const *
makeConstLowerBound(Oid type)
{
	Datum value;
	TypeCacheEntry *tce = lookup_type_cache(type, 0);

	switch (type)
	{
		case INT2OID:
			value = Int16GetDatum(PG_INT16_MIN);
			break;
		case DATEOID:
		case INT4OID:
			value = Int32GetDatum(PG_INT32_MIN);
			break;
		case INT8OID:
			value = Int64GetDatum(PG_INT64_MIN);
			break;
		case TIMESTAMPTZOID:
		case TIMESTAMPOID:
			value = Int64GetDatum(PG_INT64_MIN);
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("unsupported datatype for continuous aggregates: %s",
							format_type_be(type))));
			pg_unreachable();
	}
	return makeConst(type, -1, InvalidOid, tce->typlen, value, false, tce->typbyval);
}

static Node *
build_union_query_quals(int32 ht_id, Oid partcoltype, Oid opno, int varno, AttrNumber attno)
{
	Var *var = makeVar(varno, attno, partcoltype, -1, InvalidOid, InvalidOid);
	Oid argtyp[] = { OIDOID };
	Expr *boundary;

	Oid boundary_func_oid =
		LookupFuncName(list_make2(makeString(INTERNAL_SCHEMA_NAME), makeString(BOUNDARY_FUNCTION)),
					   lengthof(argtyp),
					   argtyp,
					   false);
	List *func_args =
		list_make1(makeConst(INT4OID, -1, InvalidOid, 4, Int32GetDatum(ht_id), false, true));

	boundary = (Expr *) makeFuncExpr(boundary_func_oid,
									 INT8OID,
									 func_args,
									 InvalidOid,
									 InvalidOid,
									 COERCE_EXPLICIT_CALL);

	/*
	 * if the partitioning column type is not integer we need to convert to proper representation
	 */
	switch (partcoltype)
	{
		case INT2OID:
		case INT4OID:
		case INT8OID:
			/* nothing to do for int types */
			break;
		case DATEOID:
		{
			Oid argtyp[] = { INT8OID };
			/* timestamp types need to be converted since we store them differently from postgres
			 * format */
			Oid converter_oid = LookupFuncName(list_make2(makeString(INTERNAL_SCHEMA_NAME),
														  makeString(INTERNAL_TO_DATE_FUNCTION)),
											   lengthof(argtyp),
											   argtyp,
											   false);
			boundary = (Expr *) makeFuncExpr(converter_oid,
											 partcoltype,
											 list_make1(boundary),
											 InvalidOid,
											 InvalidOid,
											 COERCE_EXPLICIT_CALL);

			break;
		}
		case TIMESTAMPOID:
		{
			Oid argtyp[] = { INT8OID };
			/* timestamp types need to be converted since we store them differently from postgres
			 * format */
			Oid converter_oid = LookupFuncName(list_make2(makeString(INTERNAL_SCHEMA_NAME),
														  makeString(INTERNAL_TO_TS_FUNCTION)),
											   lengthof(argtyp),
											   argtyp,
											   false);
			boundary = (Expr *) makeFuncExpr(converter_oid,
											 partcoltype,
											 list_make1(boundary),
											 InvalidOid,
											 InvalidOid,
											 COERCE_EXPLICIT_CALL);

			break;
		}
		case TIMESTAMPTZOID:
		{
			Oid argtyp[] = { INT8OID };
			/* timestamp types need to be converted since we store them differently from postgres
			 * format */
			Oid converter_oid = LookupFuncName(list_make2(makeString(INTERNAL_SCHEMA_NAME),
														  makeString(INTERNAL_TO_TSTZ_FUNCTION)),
											   lengthof(argtyp),
											   argtyp,
											   false);
			boundary = (Expr *) makeFuncExpr(converter_oid,
											 partcoltype,
											 list_make1(boundary),
											 InvalidOid,
											 InvalidOid,
											 COERCE_EXPLICIT_CALL);

			break;
		}

		default:
			/* all valid types should be handled above, this should never be reached and error
			 * handling at earlier stages should catch this */
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("unsupported datatype for continuous aggregates: %s",
							format_type_be(partcoltype))));
			pg_unreachable();
	}

	CoalesceExpr *coalesce = makeNode(CoalesceExpr);
	coalesce->coalescetype = partcoltype;
	coalesce->coalescecollid = InvalidOid;
	coalesce->args = list_make2(boundary, makeConstLowerBound(partcoltype));

	Expr *op = make_opclause(opno,
							 BOOLOID,
							 false,
							 (Expr *) var,
							 (Expr *) coalesce,
							 InvalidOid,
							 InvalidOid);

	return (Node *) op;
}

static RangeTblEntry *
makeRangeTableEntryForSubquery(Query *subquery, const char *aliasname)
{
	RangeTblEntry *rte = makeNode(RangeTblEntry);
	ListCell *lc;

	rte->rtekind = RTE_SUBQUERY;
	rte->relid = InvalidOid;
	rte->subquery = subquery;
	rte->alias = makeAlias(aliasname, NIL);
	rte->eref = copyObject(rte->alias);

	foreach (lc, subquery->targetList)
	{
		TargetEntry *tle = lfirst(lc);
		if (!tle->resjunk)
			rte->eref->colnames = lappend(rte->eref->colnames, makeString(pstrdup(tle->resname)));
	}

	/*
	 * Set flags and access permissions.
	 *
	 * Subqueries are never checked for access rights.
	 */
	rte->lateral = false;
	rte->inh = false; /* never true for subqueries */
	rte->inFromCl = true;

	rte->requiredPerms = 0;
	rte->checkAsUser = InvalidOid;
	rte->selectedCols = NULL;
	rte->insertedCols = NULL;
	rte->updatedCols = NULL;

	return rte;
}

Query *
cagg_build_union_query(CAggTimebucketInfo *tbinfo, MatTableColumnInfo *mattblinfo, Query *q1,
					   Query *q2)
{
	ListCell *lc1, *lc2;
	List *col_types = NIL;
	List *col_typmods = NIL;
	List *col_collations = NIL;
	List *tlist = NIL;
	int varno;
	AttrNumber attno;

	Assert(list_length(q1->targetList) == list_length(q2->targetList));

	q1 = copyObject(q1);
	q2 = copyObject(q2);

	TypeCacheEntry *tce = lookup_type_cache(tbinfo->htpartcoltype, TYPECACHE_LT_OPR);

	varno = list_length(q1->rtable);
	attno = mattblinfo->matpartcolno + 1;
	q1->jointree->quals =
		build_union_query_quals(tbinfo->htid, tbinfo->htpartcoltype, tce->lt_opr, varno, attno);
	attno =
		get_attnum(tbinfo->htoid, get_attname_compat(tbinfo->htoid, tbinfo->htpartcolno, false));
	varno = list_length(q2->rtable);
	q2->jointree->quals = build_union_query_quals(tbinfo->htid,
												  tbinfo->htpartcoltype,
												  get_commutator(tce->lt_opr),
												  varno,
												  attno);

	Query *query = NULL;
	SetOperationStmt *setop = makeNode(SetOperationStmt);
	RangeTblEntry *rte_q1 = makeRangeTableEntryForSubquery(q1, "*SELECT* 1");
	RangeTblEntry *rte_q2 = makeRangeTableEntryForSubquery(q2, "*SELECT* 2");
	RangeTblRef *ref_q1 = makeNode(RangeTblRef);
	RangeTblRef *ref_q2 = makeNode(RangeTblRef);
	query = makeNode(Query);
	query->commandType = CMD_SELECT;
	query->querySource = 0;
	query->queryId = q1->queryId;
	query->canSetTag = q1->canSetTag;
	query->utilityStmt = NULL;
	query->resultRelation = 0;
	query->hasAggs = false;
	query->hasRowSecurity = false;

	query->rtable = list_make2(rte_q1, rte_q2);
	query->setOperations = (Node *) setop;

	setop->op = SETOP_UNION;
	setop->all = true;
	ref_q1->rtindex = 1;
	ref_q2->rtindex = 2;
	setop->larg = (Node *) ref_q1;
	setop->rarg = (Node *) ref_q2;

	forboth (lc1, q1->targetList, lc2, q2->targetList)
	{
		TargetEntry *tle = lfirst(lc1);
		TargetEntry *tle2 = lfirst(lc2);
		TargetEntry *tle_union;
		Var *expr;
		if (!tle->resjunk)
		{
			col_types = lappend_int(col_types, exprType((Node *) tle->expr));
			col_typmods = lappend_int(col_typmods, exprTypmod((Node *) tle->expr));
			col_collations = lappend_int(col_collations, exprCollation((Node *) tle->expr));

			expr = makeVarFromTargetEntry(1, tle);
			tle_union = makeTargetEntry((Expr *) copyObject(expr),
										list_length(tlist) + 1,
										tle2->resname,
										false);
			tle_union->resorigtbl = expr->varno;
			tle_union->resorigcol = expr->varattno;
			tlist = lappend(tlist, tle_union);
		}
	}

	query->targetList = tlist;

	setop->colTypes = col_types;
	setop->colTypmods = col_typmods;
	setop->colCollations = col_collations;

	return query;
}
