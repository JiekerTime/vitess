/*
Copyright 2023 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
// nolint
package sqlparser

// CloneSQLNode creates a deep clone of the input.
func DeepCloneSQLNode(in SQLNode) SQLNode {
	if in == nil {
		return nil
	}
	switch in := in.(type) {
	case *AddColumns:
		return DeepCloneRefOfAddColumns(in)
	case *AddConstraintDefinition:
		return DeepCloneRefOfAddConstraintDefinition(in)
	case *AddIndexDefinition:
		return DeepCloneRefOfAddIndexDefinition(in)
	case AlgorithmValue:
		return in
	case *AliasedExpr:
		return DeepCloneRefOfAliasedExpr(in)
	case *AliasedTableExpr:
		return DeepCloneRefOfAliasedTableExpr(in)
	case *AlterCharset:
		return DeepCloneRefOfAlterCharset(in)
	case *AlterCheck:
		return DeepCloneRefOfAlterCheck(in)
	case *AlterColumn:
		return DeepCloneRefOfAlterColumn(in)
	case *AlterDatabase:
		return DeepCloneRefOfAlterDatabase(in)
	case *AlterIndex:
		return DeepCloneRefOfAlterIndex(in)
	case *AlterMigration:
		return DeepCloneRefOfAlterMigration(in)
	case *AlterTable:
		return DeepCloneRefOfAlterTable(in)
	case *AlterView:
		return DeepCloneRefOfAlterView(in)
	case *AlterVschema:
		return DeepCloneRefOfAlterVschema(in)
	case *Analyze:
		return DeepCloneRefOfAnalyze(in)
	case *AndExpr:
		return DeepCloneRefOfAndExpr(in)
	case *AnyValue:
		return DeepCloneRefOfAnyValue(in)
	case *Argument:
		return DeepCloneRefOfArgument(in)
	case *ArgumentLessWindowExpr:
		return DeepCloneRefOfArgumentLessWindowExpr(in)
	case *AssignmentExpr:
		return DeepCloneRefOfAssignmentExpr(in)
	case *AutoIncSpec:
		return DeepCloneRefOfAutoIncSpec(in)
	case *Avg:
		return DeepCloneRefOfAvg(in)
	case *Begin:
		return DeepCloneRefOfBegin(in)
	case *BetweenExpr:
		return DeepCloneRefOfBetweenExpr(in)
	case *BinaryExpr:
		return DeepCloneRefOfBinaryExpr(in)
	case *BitAnd:
		return DeepCloneRefOfBitAnd(in)
	case *BitOr:
		return DeepCloneRefOfBitOr(in)
	case *BitXor:
		return DeepCloneRefOfBitXor(in)
	case BoolVal:
		return in
	case *CallProc:
		return DeepCloneRefOfCallProc(in)
	case *CaseExpr:
		return DeepCloneRefOfCaseExpr(in)
	case *CastExpr:
		return DeepCloneRefOfCastExpr(in)
	case *ChangeColumn:
		return DeepCloneRefOfChangeColumn(in)
	case *CharExpr:
		return DeepCloneRefOfCharExpr(in)
	case *CheckConstraintDefinition:
		return DeepCloneRefOfCheckConstraintDefinition(in)
	case *ColName:
		return DeepCloneRefOfColName(in)
	case *CollateExpr:
		return DeepCloneRefOfCollateExpr(in)
	case *ColumnDefinition:
		return DeepCloneRefOfColumnDefinition(in)
	case *ColumnType:
		return DeepCloneRefOfColumnType(in)
	case Columns:
		return DeepCloneColumns(in)
	case *CommentOnly:
		return DeepCloneRefOfCommentOnly(in)
	case *Commit:
		return DeepCloneRefOfCommit(in)
	case *CommonTableExpr:
		return DeepCloneRefOfCommonTableExpr(in)
	case *ComparisonExpr:
		return DeepCloneRefOfComparisonExpr(in)
	case *ConstraintDefinition:
		return DeepCloneRefOfConstraintDefinition(in)
	case *ConvertExpr:
		return DeepCloneRefOfConvertExpr(in)
	case *ConvertType:
		return DeepCloneRefOfConvertType(in)
	case *ConvertUsingExpr:
		return DeepCloneRefOfConvertUsingExpr(in)
	case *Count:
		return DeepCloneRefOfCount(in)
	case *CountStar:
		return DeepCloneRefOfCountStar(in)
	case *CreateDatabase:
		return DeepCloneRefOfCreateDatabase(in)
	case *CreateTable:
		return DeepCloneRefOfCreateTable(in)
	case *CreateView:
		return DeepCloneRefOfCreateView(in)
	case *CurTimeFuncExpr:
		return DeepCloneRefOfCurTimeFuncExpr(in)
	case *DeallocateStmt:
		return DeepCloneRefOfDeallocateStmt(in)
	case *Default:
		return DeepCloneRefOfDefault(in)
	case *Definer:
		return DeepCloneRefOfDefiner(in)
	case *Delete:
		return DeepCloneRefOfDelete(in)
	case *DerivedTable:
		return DeepCloneRefOfDerivedTable(in)
	case *DropColumn:
		return DeepCloneRefOfDropColumn(in)
	case *DropDatabase:
		return DeepCloneRefOfDropDatabase(in)
	case *DropKey:
		return DeepCloneRefOfDropKey(in)
	case *DropTable:
		return DeepCloneRefOfDropTable(in)
	case *DropView:
		return DeepCloneRefOfDropView(in)
	case *ExecuteStmt:
		return DeepCloneRefOfExecuteStmt(in)
	case *ExistsExpr:
		return DeepCloneRefOfExistsExpr(in)
	case *ExplainStmt:
		return DeepCloneRefOfExplainStmt(in)
	case *ExplainTab:
		return DeepCloneRefOfExplainTab(in)
	case Exprs:
		return DeepCloneExprs(in)
	case *ExtractFuncExpr:
		return DeepCloneRefOfExtractFuncExpr(in)
	case *ExtractValueExpr:
		return DeepCloneRefOfExtractValueExpr(in)
	case *FieldsClause:
		return DeepCloneRefOfFieldsClause(in)
	case *FirstOrLastValueExpr:
		return DeepCloneRefOfFirstOrLastValueExpr(in)
	case *Flush:
		return DeepCloneRefOfFlush(in)
	case *Force:
		return DeepCloneRefOfForce(in)
	case *ForeignKeyDefinition:
		return DeepCloneRefOfForeignKeyDefinition(in)
	case *FrameClause:
		return DeepCloneRefOfFrameClause(in)
	case *FramePoint:
		return DeepCloneRefOfFramePoint(in)
	case *FromFirstLastClause:
		return DeepCloneRefOfFromFirstLastClause(in)
	case *FuncExpr:
		return DeepCloneRefOfFuncExpr(in)
	case *GTIDFuncExpr:
		return DeepCloneRefOfGTIDFuncExpr(in)
	case *GeoHashFromLatLongExpr:
		return DeepCloneRefOfGeoHashFromLatLongExpr(in)
	case *GeoHashFromPointExpr:
		return DeepCloneRefOfGeoHashFromPointExpr(in)
	case *GeoJSONFromGeomExpr:
		return DeepCloneRefOfGeoJSONFromGeomExpr(in)
	case *GeomCollPropertyFuncExpr:
		return DeepCloneRefOfGeomCollPropertyFuncExpr(in)
	case *GeomFormatExpr:
		return DeepCloneRefOfGeomFormatExpr(in)
	case *GeomFromGeoHashExpr:
		return DeepCloneRefOfGeomFromGeoHashExpr(in)
	case *GeomFromGeoJSONExpr:
		return DeepCloneRefOfGeomFromGeoJSONExpr(in)
	case *GeomFromTextExpr:
		return DeepCloneRefOfGeomFromTextExpr(in)
	case *GeomFromWKBExpr:
		return DeepCloneRefOfGeomFromWKBExpr(in)
	case *GeomPropertyFuncExpr:
		return DeepCloneRefOfGeomPropertyFuncExpr(in)
	case GroupBy:
		return DeepCloneGroupBy(in)
	case *GroupConcatExpr:
		return DeepCloneRefOfGroupConcatExpr(in)
	case IdentifierCI:
		return DeepCloneIdentifierCI(in)
	case IdentifierCS:
		return DeepCloneIdentifierCS(in)
	case *IndexDefinition:
		return DeepCloneRefOfIndexDefinition(in)
	case *IndexHint:
		return DeepCloneRefOfIndexHint(in)
	case IndexHints:
		return DeepCloneIndexHints(in)
	case *IndexInfo:
		return DeepCloneRefOfIndexInfo(in)
	case *Insert:
		return DeepCloneRefOfInsert(in)
	case *InsertExpr:
		return DeepCloneRefOfInsertExpr(in)
	case *IntervalDateExpr:
		return DeepCloneRefOfIntervalDateExpr(in)
	case *IntervalFuncExpr:
		return DeepCloneRefOfIntervalFuncExpr(in)
	case *IntroducerExpr:
		return DeepCloneRefOfIntroducerExpr(in)
	case *IsExpr:
		return DeepCloneRefOfIsExpr(in)
	case *JSONArrayExpr:
		return DeepCloneRefOfJSONArrayExpr(in)
	case *JSONAttributesExpr:
		return DeepCloneRefOfJSONAttributesExpr(in)
	case *JSONContainsExpr:
		return DeepCloneRefOfJSONContainsExpr(in)
	case *JSONContainsPathExpr:
		return DeepCloneRefOfJSONContainsPathExpr(in)
	case *JSONExtractExpr:
		return DeepCloneRefOfJSONExtractExpr(in)
	case *JSONKeysExpr:
		return DeepCloneRefOfJSONKeysExpr(in)
	case *JSONObjectExpr:
		return DeepCloneRefOfJSONObjectExpr(in)
	case *JSONObjectParam:
		return DeepCloneRefOfJSONObjectParam(in)
	case *JSONOverlapsExpr:
		return DeepCloneRefOfJSONOverlapsExpr(in)
	case *JSONPrettyExpr:
		return DeepCloneRefOfJSONPrettyExpr(in)
	case *JSONQuoteExpr:
		return DeepCloneRefOfJSONQuoteExpr(in)
	case *JSONRemoveExpr:
		return DeepCloneRefOfJSONRemoveExpr(in)
	case *JSONSchemaValidFuncExpr:
		return DeepCloneRefOfJSONSchemaValidFuncExpr(in)
	case *JSONSchemaValidationReportFuncExpr:
		return DeepCloneRefOfJSONSchemaValidationReportFuncExpr(in)
	case *JSONSearchExpr:
		return DeepCloneRefOfJSONSearchExpr(in)
	case *JSONStorageFreeExpr:
		return DeepCloneRefOfJSONStorageFreeExpr(in)
	case *JSONStorageSizeExpr:
		return DeepCloneRefOfJSONStorageSizeExpr(in)
	case *JSONTableExpr:
		return DeepCloneRefOfJSONTableExpr(in)
	case *JSONUnquoteExpr:
		return DeepCloneRefOfJSONUnquoteExpr(in)
	case *JSONValueExpr:
		return DeepCloneRefOfJSONValueExpr(in)
	case *JSONValueMergeExpr:
		return DeepCloneRefOfJSONValueMergeExpr(in)
	case *JSONValueModifierExpr:
		return DeepCloneRefOfJSONValueModifierExpr(in)
	case *JoinCondition:
		return DeepCloneRefOfJoinCondition(in)
	case *JoinTableExpr:
		return DeepCloneRefOfJoinTableExpr(in)
	case *JtColumnDefinition:
		return DeepCloneRefOfJtColumnDefinition(in)
	case *JtOnResponse:
		return DeepCloneRefOfJtOnResponse(in)
	case *KeyState:
		return DeepCloneRefOfKeyState(in)
	case *Kill:
		return DeepCloneRefOfKill(in)
	case *LagLeadExpr:
		return DeepCloneRefOfLagLeadExpr(in)
	case *Limit:
		return DeepCloneRefOfLimit(in)
	case *LineStringExpr:
		return DeepCloneRefOfLineStringExpr(in)
	case *LinesClause:
		return DeepCloneRefOfLinesClause(in)
	case *LinestrPropertyFuncExpr:
		return DeepCloneRefOfLinestrPropertyFuncExpr(in)
	case ListArg:
		return in
	case *Literal:
		return DeepCloneRefOfLiteral(in)
	case *Load:
		return DeepCloneRefOfLoad(in)
	case *LoadDataStmt:
		return DeepCloneRefOfLoadDataStmt(in)
	case *LocateExpr:
		return DeepCloneRefOfLocateExpr(in)
	case *LockOption:
		return DeepCloneRefOfLockOption(in)
	case *LockTables:
		return DeepCloneRefOfLockTables(in)
	case *LockingFunc:
		return DeepCloneRefOfLockingFunc(in)
	case MatchAction:
		return in
	case *MatchExpr:
		return DeepCloneRefOfMatchExpr(in)
	case *Max:
		return DeepCloneRefOfMax(in)
	case *MemberOfExpr:
		return DeepCloneRefOfMemberOfExpr(in)
	case *Min:
		return DeepCloneRefOfMin(in)
	case *ModifyColumn:
		return DeepCloneRefOfModifyColumn(in)
	case *MultiLinestringExpr:
		return DeepCloneRefOfMultiLinestringExpr(in)
	case *MultiPointExpr:
		return DeepCloneRefOfMultiPointExpr(in)
	case *MultiPolygonExpr:
		return DeepCloneRefOfMultiPolygonExpr(in)
	case *NTHValueExpr:
		return DeepCloneRefOfNTHValueExpr(in)
	case *NamedWindow:
		return DeepCloneRefOfNamedWindow(in)
	case NamedWindows:
		return DeepCloneNamedWindows(in)
	case *Nextval:
		return DeepCloneRefOfNextval(in)
	case *NotExpr:
		return DeepCloneRefOfNotExpr(in)
	case *NtileExpr:
		return DeepCloneRefOfNtileExpr(in)
	case *NullTreatmentClause:
		return DeepCloneRefOfNullTreatmentClause(in)
	case *NullVal:
		return DeepCloneRefOfNullVal(in)
	case *Offset:
		return DeepCloneRefOfOffset(in)
	case OnDup:
		return DeepCloneOnDup(in)
	case *OptLike:
		return DeepCloneRefOfOptLike(in)
	case *OrExpr:
		return DeepCloneRefOfOrExpr(in)
	case *Order:
		return DeepCloneRefOfOrder(in)
	case OrderBy:
		return DeepCloneOrderBy(in)
	case *OrderByOption:
		return DeepCloneRefOfOrderByOption(in)
	case *OtherAdmin:
		return DeepCloneRefOfOtherAdmin(in)
	case *OverClause:
		return DeepCloneRefOfOverClause(in)
	case *ParenTableExpr:
		return DeepCloneRefOfParenTableExpr(in)
	case *ParsedComments:
		return DeepCloneRefOfParsedComments(in)
	case *PartitionDefinition:
		return DeepCloneRefOfPartitionDefinition(in)
	case *PartitionDefinitionOptions:
		return DeepCloneRefOfPartitionDefinitionOptions(in)
	case *PartitionEngine:
		return DeepCloneRefOfPartitionEngine(in)
	case *PartitionOption:
		return DeepCloneRefOfPartitionOption(in)
	case *PartitionSpec:
		return DeepCloneRefOfPartitionSpec(in)
	case *PartitionValueRange:
		return DeepCloneRefOfPartitionValueRange(in)
	case Partitions:
		return DeepClonePartitions(in)
	case *PerformanceSchemaFuncExpr:
		return DeepCloneRefOfPerformanceSchemaFuncExpr(in)
	case *PointExpr:
		return DeepCloneRefOfPointExpr(in)
	case *PointPropertyFuncExpr:
		return DeepCloneRefOfPointPropertyFuncExpr(in)
	case *PolygonExpr:
		return DeepCloneRefOfPolygonExpr(in)
	case *PolygonPropertyFuncExpr:
		return DeepCloneRefOfPolygonPropertyFuncExpr(in)
	case *PrepareStmt:
		return DeepCloneRefOfPrepareStmt(in)
	case *PurgeBinaryLogs:
		return DeepCloneRefOfPurgeBinaryLogs(in)
	case ReferenceAction:
		return in
	case *ReferenceDefinition:
		return DeepCloneRefOfReferenceDefinition(in)
	case *RegexpInstrExpr:
		return DeepCloneRefOfRegexpInstrExpr(in)
	case *RegexpLikeExpr:
		return DeepCloneRefOfRegexpLikeExpr(in)
	case *RegexpReplaceExpr:
		return DeepCloneRefOfRegexpReplaceExpr(in)
	case *RegexpSubstrExpr:
		return DeepCloneRefOfRegexpSubstrExpr(in)
	case *Release:
		return DeepCloneRefOfRelease(in)
	case *RenameColumn:
		return DeepCloneRefOfRenameColumn(in)
	case *RenameIndex:
		return DeepCloneRefOfRenameIndex(in)
	case *RenameTable:
		return DeepCloneRefOfRenameTable(in)
	case *RenameTableName:
		return DeepCloneRefOfRenameTableName(in)
	case *RevertMigration:
		return DeepCloneRefOfRevertMigration(in)
	case *Rollback:
		return DeepCloneRefOfRollback(in)
	case RootNode:
		return DeepCloneRootNode(in)
	case *SRollback:
		return DeepCloneRefOfSRollback(in)
	case *Savepoint:
		return DeepCloneRefOfSavepoint(in)
	case *Select:
		return DeepCloneRefOfSelect(in)
	case SelectExprs:
		return DeepCloneSelectExprs(in)
	case *SelectInto:
		return DeepCloneRefOfSelectInto(in)
	case *Set:
		return DeepCloneRefOfSet(in)
	case *SetExpr:
		return DeepCloneRefOfSetExpr(in)
	case SetExprs:
		return DeepCloneSetExprs(in)
	case *Show:
		return DeepCloneRefOfShow(in)
	case *ShowBasic:
		return DeepCloneRefOfShowBasic(in)
	case *ShowCreate:
		return DeepCloneRefOfShowCreate(in)
	case *ShowFilter:
		return DeepCloneRefOfShowFilter(in)
	case *ShowMigrationLogs:
		return DeepCloneRefOfShowMigrationLogs(in)
	case *ShowOther:
		return DeepCloneRefOfShowOther(in)
	case *ShowThrottledApps:
		return DeepCloneRefOfShowThrottledApps(in)
	case *ShowThrottlerStatus:
		return DeepCloneRefOfShowThrottlerStatus(in)
	case *StarExpr:
		return DeepCloneRefOfStarExpr(in)
	case *Std:
		return DeepCloneRefOfStd(in)
	case *StdDev:
		return DeepCloneRefOfStdDev(in)
	case *StdPop:
		return DeepCloneRefOfStdPop(in)
	case *StdSamp:
		return DeepCloneRefOfStdSamp(in)
	case *Stream:
		return DeepCloneRefOfStream(in)
	case *SubPartition:
		return DeepCloneRefOfSubPartition(in)
	case *SubPartitionDefinition:
		return DeepCloneRefOfSubPartitionDefinition(in)
	case *SubPartitionDefinitionOptions:
		return DeepCloneRefOfSubPartitionDefinitionOptions(in)
	case SubPartitionDefinitions:
		return DeepCloneSubPartitionDefinitions(in)
	case *Subquery:
		return DeepCloneRefOfSubquery(in)
	case *SubstrExpr:
		return DeepCloneRefOfSubstrExpr(in)
	case *Sum:
		return DeepCloneRefOfSum(in)
	case TableExprs:
		return DeepCloneTableExprs(in)
	case TableName:
		return DeepCloneTableName(in)
	case TableNames:
		return DeepCloneTableNames(in)
	case TableOptions:
		return DeepCloneTableOptions(in)
	case *TableSpec:
		return DeepCloneRefOfTableSpec(in)
	case *TablespaceOperation:
		return DeepCloneRefOfTablespaceOperation(in)
	case *TimestampDiffExpr:
		return DeepCloneRefOfTimestampDiffExpr(in)
	case *TrimFuncExpr:
		return DeepCloneRefOfTrimFuncExpr(in)
	case *TruncateTable:
		return DeepCloneRefOfTruncateTable(in)
	case *UnaryExpr:
		return DeepCloneRefOfUnaryExpr(in)
	case *Union:
		return DeepCloneRefOfUnion(in)
	case *UnlockTables:
		return DeepCloneRefOfUnlockTables(in)
	case *Update:
		return DeepCloneRefOfUpdate(in)
	case *UpdateExpr:
		return DeepCloneRefOfUpdateExpr(in)
	case UpdateExprs:
		return DeepCloneUpdateExprs(in)
	case *UpdateXMLExpr:
		return DeepCloneRefOfUpdateXMLExpr(in)
	case *Use:
		return DeepCloneRefOfUse(in)
	case *VExplainStmt:
		return DeepCloneRefOfVExplainStmt(in)
	case *VStream:
		return DeepCloneRefOfVStream(in)
	case ValTuple:
		return DeepCloneValTuple(in)
	case *Validation:
		return DeepCloneRefOfValidation(in)
	case Values:
		return DeepCloneValues(in)
	case *ValuesFuncExpr:
		return DeepCloneRefOfValuesFuncExpr(in)
	case *VarPop:
		return DeepCloneRefOfVarPop(in)
	case *VarSamp:
		return DeepCloneRefOfVarSamp(in)
	case *Variable:
		return DeepCloneRefOfVariable(in)
	case *Variance:
		return DeepCloneRefOfVariance(in)
	case VindexParam:
		return DeepCloneVindexParam(in)
	case *VindexSpec:
		return DeepCloneRefOfVindexSpec(in)
	case *WeightStringFuncExpr:
		return DeepCloneRefOfWeightStringFuncExpr(in)
	case *When:
		return DeepCloneRefOfWhen(in)
	case *Where:
		return DeepCloneRefOfWhere(in)
	case *WindowDefinition:
		return DeepCloneRefOfWindowDefinition(in)
	case WindowDefinitions:
		return DeepCloneWindowDefinitions(in)
	case *WindowSpecification:
		return DeepCloneRefOfWindowSpecification(in)
	case *With:
		return DeepCloneRefOfWith(in)
	case *XorExpr:
		return DeepCloneRefOfXorExpr(in)
	default:
		// this should never happen
		return nil
	}
}

// CloneRefOfAddColumns creates a deep clone of the input.
func DeepCloneRefOfAddColumns(n *AddColumns) *AddColumns {
	if n == nil {
		return nil
	}
	out := *n
	out.Columns = DeepCloneSliceOfRefOfColumnDefinition(n.Columns)
	out.After = DeepCloneRefOfColName(n.After)
	return &out
}

// CloneRefOfAddConstraintDefinition creates a deep clone of the input.
func DeepCloneRefOfAddConstraintDefinition(n *AddConstraintDefinition) *AddConstraintDefinition {
	if n == nil {
		return nil
	}
	out := *n
	out.ConstraintDefinition = DeepCloneRefOfConstraintDefinition(n.ConstraintDefinition)
	return &out
}

// CloneRefOfAddIndexDefinition creates a deep clone of the input.
func DeepCloneRefOfAddIndexDefinition(n *AddIndexDefinition) *AddIndexDefinition {
	if n == nil {
		return nil
	}
	out := *n
	out.IndexDefinition = DeepCloneRefOfIndexDefinition(n.IndexDefinition)
	return &out
}

// CloneRefOfAliasedExpr creates a deep clone of the input.
func DeepCloneRefOfAliasedExpr(n *AliasedExpr) *AliasedExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Expr = DeepCloneExpr(n.Expr)
	out.As = DeepCloneIdentifierCI(n.As)
	return &out
}

// CloneRefOfAliasedTableExpr creates a deep clone of the input.
func DeepCloneRefOfAliasedTableExpr(n *AliasedTableExpr) *AliasedTableExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Expr = DeepCloneSimpleTableExpr(n.Expr)
	out.Partitions = DeepClonePartitions(n.Partitions)
	out.As = DeepCloneIdentifierCS(n.As)
	out.Hints = DeepCloneIndexHints(n.Hints)
	out.Columns = DeepCloneColumns(n.Columns)
	return &out
}

// CloneRefOfAlterCharset creates a deep clone of the input.
func DeepCloneRefOfAlterCharset(n *AlterCharset) *AlterCharset {
	if n == nil {
		return nil
	}
	out := *n
	return &out
}

// CloneRefOfAlterCheck creates a deep clone of the input.
func DeepCloneRefOfAlterCheck(n *AlterCheck) *AlterCheck {
	if n == nil {
		return nil
	}
	out := *n
	out.Name = DeepCloneIdentifierCI(n.Name)
	return &out
}

// CloneRefOfAlterColumn creates a deep clone of the input.
func DeepCloneRefOfAlterColumn(n *AlterColumn) *AlterColumn {
	if n == nil {
		return nil
	}
	out := *n
	out.Column = DeepCloneRefOfColName(n.Column)
	out.DefaultVal = DeepCloneExpr(n.DefaultVal)
	out.Invisible = DeepCloneRefOfBool(n.Invisible)
	return &out
}

// CloneRefOfAlterDatabase creates a deep clone of the input.
func DeepCloneRefOfAlterDatabase(n *AlterDatabase) *AlterDatabase {
	if n == nil {
		return nil
	}
	out := *n
	out.DBName = DeepCloneIdentifierCS(n.DBName)
	out.AlterOptions = DeepCloneSliceOfDatabaseOption(n.AlterOptions)
	return &out
}

// CloneRefOfAlterIndex creates a deep clone of the input.
func DeepCloneRefOfAlterIndex(n *AlterIndex) *AlterIndex {
	if n == nil {
		return nil
	}
	out := *n
	out.Name = DeepCloneIdentifierCI(n.Name)
	return &out
}

// CloneRefOfAlterMigration creates a deep clone of the input.
func DeepCloneRefOfAlterMigration(n *AlterMigration) *AlterMigration {
	if n == nil {
		return nil
	}
	out := *n
	out.Ratio = DeepCloneRefOfLiteral(n.Ratio)
	return &out
}

// CloneRefOfAlterTable creates a deep clone of the input.
func DeepCloneRefOfAlterTable(n *AlterTable) *AlterTable {
	if n == nil {
		return nil
	}
	out := *n
	out.Table = DeepCloneTableName(n.Table)
	out.AlterOptions = DeepCloneSliceOfAlterOption(n.AlterOptions)
	out.PartitionSpec = DeepCloneRefOfPartitionSpec(n.PartitionSpec)
	out.PartitionOption = DeepCloneRefOfPartitionOption(n.PartitionOption)
	out.Comments = DeepCloneRefOfParsedComments(n.Comments)
	return &out
}

// CloneRefOfAlterView creates a deep clone of the input.
func DeepCloneRefOfAlterView(n *AlterView) *AlterView {
	if n == nil {
		return nil
	}
	out := *n
	out.ViewName = DeepCloneTableName(n.ViewName)
	out.Definer = DeepCloneRefOfDefiner(n.Definer)
	out.Columns = DeepCloneColumns(n.Columns)
	out.Select = DeepCloneSelectStatement(n.Select)
	out.Comments = DeepCloneRefOfParsedComments(n.Comments)
	return &out
}

// CloneRefOfAlterVschema creates a deep clone of the input.
func DeepCloneRefOfAlterVschema(n *AlterVschema) *AlterVschema {
	if n == nil {
		return nil
	}
	out := *n
	out.Table = DeepCloneTableName(n.Table)
	out.VindexSpec = DeepCloneRefOfVindexSpec(n.VindexSpec)
	out.VindexCols = DeepCloneSliceOfIdentifierCI(n.VindexCols)
	out.AutoIncSpec = DeepCloneRefOfAutoIncSpec(n.AutoIncSpec)
	return &out
}

// CloneRefOfAnalyze creates a deep clone of the input.
func DeepCloneRefOfAnalyze(n *Analyze) *Analyze {
	if n == nil {
		return nil
	}
	out := *n
	out.Table = DeepCloneTableName(n.Table)
	return &out
}

// CloneRefOfAndExpr creates a deep clone of the input.
func DeepCloneRefOfAndExpr(n *AndExpr) *AndExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Left = DeepCloneExpr(n.Left)
	out.Right = DeepCloneExpr(n.Right)
	return &out
}

// CloneRefOfAnyValue creates a deep clone of the input.
func DeepCloneRefOfAnyValue(n *AnyValue) *AnyValue {
	if n == nil {
		return nil
	}
	out := *n
	out.Arg = DeepCloneExpr(n.Arg)
	return &out
}

// CloneRefOfArgument creates a deep clone of the input.
func DeepCloneRefOfArgument(n *Argument) *Argument {
	if n == nil {
		return nil
	}
	out := *n
	return &out
}

// CloneRefOfArgumentLessWindowExpr creates a deep clone of the input.
func DeepCloneRefOfArgumentLessWindowExpr(n *ArgumentLessWindowExpr) *ArgumentLessWindowExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.OverClause = DeepCloneRefOfOverClause(n.OverClause)
	return &out
}

// CloneRefOfAssignmentExpr creates a deep clone of the input.
func DeepCloneRefOfAssignmentExpr(n *AssignmentExpr) *AssignmentExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Left = DeepCloneExpr(n.Left)
	out.Right = DeepCloneExpr(n.Right)
	return &out
}

// CloneRefOfAutoIncSpec creates a deep clone of the input.
func DeepCloneRefOfAutoIncSpec(n *AutoIncSpec) *AutoIncSpec {
	if n == nil {
		return nil
	}
	out := *n
	out.Column = DeepCloneIdentifierCI(n.Column)
	out.Sequence = DeepCloneTableName(n.Sequence)
	return &out
}

// CloneRefOfAvg creates a deep clone of the input.
func DeepCloneRefOfAvg(n *Avg) *Avg {
	if n == nil {
		return nil
	}
	out := *n
	out.Arg = DeepCloneExpr(n.Arg)
	return &out
}

// CloneRefOfBegin creates a deep clone of the input.
func DeepCloneRefOfBegin(n *Begin) *Begin {
	if n == nil {
		return nil
	}
	out := *n
	out.TxAccessModes = DeepCloneSliceOfTxAccessMode(n.TxAccessModes)
	return &out
}

// CloneRefOfBetweenExpr creates a deep clone of the input.
func DeepCloneRefOfBetweenExpr(n *BetweenExpr) *BetweenExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Left = DeepCloneExpr(n.Left)
	out.From = DeepCloneExpr(n.From)
	out.To = DeepCloneExpr(n.To)
	return &out
}

// CloneRefOfBinaryExpr creates a deep clone of the input.
func DeepCloneRefOfBinaryExpr(n *BinaryExpr) *BinaryExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Left = DeepCloneExpr(n.Left)
	out.Right = DeepCloneExpr(n.Right)
	return &out
}

// CloneRefOfBitAnd creates a deep clone of the input.
func DeepCloneRefOfBitAnd(n *BitAnd) *BitAnd {
	if n == nil {
		return nil
	}
	out := *n
	out.Arg = DeepCloneExpr(n.Arg)
	return &out
}

// CloneRefOfBitOr creates a deep clone of the input.
func DeepCloneRefOfBitOr(n *BitOr) *BitOr {
	if n == nil {
		return nil
	}
	out := *n
	out.Arg = DeepCloneExpr(n.Arg)
	return &out
}

// CloneRefOfBitXor creates a deep clone of the input.
func DeepCloneRefOfBitXor(n *BitXor) *BitXor {
	if n == nil {
		return nil
	}
	out := *n
	out.Arg = DeepCloneExpr(n.Arg)
	return &out
}

// CloneRefOfCallProc creates a deep clone of the input.
func DeepCloneRefOfCallProc(n *CallProc) *CallProc {
	if n == nil {
		return nil
	}
	out := *n
	out.Name = DeepCloneTableName(n.Name)
	out.Params = DeepCloneExprs(n.Params)
	return &out
}

// CloneRefOfCaseExpr creates a deep clone of the input.
func DeepCloneRefOfCaseExpr(n *CaseExpr) *CaseExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Expr = DeepCloneExpr(n.Expr)
	out.Whens = DeepCloneSliceOfRefOfWhen(n.Whens)
	out.Else = DeepCloneExpr(n.Else)
	return &out
}

// CloneRefOfCastExpr creates a deep clone of the input.
func DeepCloneRefOfCastExpr(n *CastExpr) *CastExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Expr = DeepCloneExpr(n.Expr)
	out.Type = DeepCloneRefOfConvertType(n.Type)
	return &out
}

// CloneRefOfChangeColumn creates a deep clone of the input.
func DeepCloneRefOfChangeColumn(n *ChangeColumn) *ChangeColumn {
	if n == nil {
		return nil
	}
	out := *n
	out.OldColumn = DeepCloneRefOfColName(n.OldColumn)
	out.NewColDefinition = DeepCloneRefOfColumnDefinition(n.NewColDefinition)
	out.After = DeepCloneRefOfColName(n.After)
	return &out
}

// CloneRefOfCharExpr creates a deep clone of the input.
func DeepCloneRefOfCharExpr(n *CharExpr) *CharExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Exprs = DeepCloneExprs(n.Exprs)
	return &out
}

// CloneRefOfCheckConstraintDefinition creates a deep clone of the input.
func DeepCloneRefOfCheckConstraintDefinition(n *CheckConstraintDefinition) *CheckConstraintDefinition {
	if n == nil {
		return nil
	}
	out := *n
	out.Expr = DeepCloneExpr(n.Expr)
	return &out
}

// CloneRefOfColName creates a deep clone of the input.
func DeepCloneRefOfColName(n *ColName) *ColName {
	if n == nil {
		return nil
	}
	out := *n
	out.Name = CloneIdentifierCI(n.Name)
	out.Qualifier = CloneTableName(n.Qualifier)
	return &out
}

// CloneRefOfCollateExpr creates a deep clone of the input.
func DeepCloneRefOfCollateExpr(n *CollateExpr) *CollateExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Expr = DeepCloneExpr(n.Expr)
	return &out
}

// CloneRefOfColumnDefinition creates a deep clone of the input.
func DeepCloneRefOfColumnDefinition(n *ColumnDefinition) *ColumnDefinition {
	if n == nil {
		return nil
	}
	out := *n
	out.Name = DeepCloneIdentifierCI(n.Name)
	out.Type = DeepCloneRefOfColumnType(n.Type)
	return &out
}

// CloneRefOfColumnType creates a deep clone of the input.
func DeepCloneRefOfColumnType(n *ColumnType) *ColumnType {
	if n == nil {
		return nil
	}
	out := *n
	out.Options = DeepCloneRefOfColumnTypeOptions(n.Options)
	out.Length = DeepCloneRefOfLiteral(n.Length)
	out.Scale = DeepCloneRefOfLiteral(n.Scale)
	out.Charset = DeepCloneColumnCharset(n.Charset)
	out.EnumValues = DeepCloneSliceOfString(n.EnumValues)
	return &out
}

// CloneColumns creates a deep clone of the input.
func DeepCloneColumns(n Columns) Columns {
	if n == nil {
		return nil
	}
	res := make(Columns, len(n))
	for i, x := range n {
		res[i] = DeepCloneIdentifierCI(x)
	}
	return res
}

// CloneRefOfCommentOnly creates a deep clone of the input.
func DeepCloneRefOfCommentOnly(n *CommentOnly) *CommentOnly {
	if n == nil {
		return nil
	}
	out := *n
	out.Comments = DeepCloneSliceOfString(n.Comments)
	return &out
}

// CloneRefOfCommit creates a deep clone of the input.
func DeepCloneRefOfCommit(n *Commit) *Commit {
	if n == nil {
		return nil
	}
	out := *n
	return &out
}

// CloneRefOfCommonTableExpr creates a deep clone of the input.
func DeepCloneRefOfCommonTableExpr(n *CommonTableExpr) *CommonTableExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.ID = DeepCloneIdentifierCS(n.ID)
	out.Columns = DeepCloneColumns(n.Columns)
	out.Subquery = DeepCloneRefOfSubquery(n.Subquery)
	return &out
}

// CloneRefOfComparisonExpr creates a deep clone of the input.
func DeepCloneRefOfComparisonExpr(n *ComparisonExpr) *ComparisonExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Left = DeepCloneExpr(n.Left)
	out.Right = DeepCloneExpr(n.Right)
	out.Escape = DeepCloneExpr(n.Escape)
	return &out
}

// CloneRefOfConstraintDefinition creates a deep clone of the input.
func DeepCloneRefOfConstraintDefinition(n *ConstraintDefinition) *ConstraintDefinition {
	if n == nil {
		return nil
	}
	out := *n
	out.Name = DeepCloneIdentifierCI(n.Name)
	out.Details = DeepCloneConstraintInfo(n.Details)
	return &out
}

// CloneRefOfConvertExpr creates a deep clone of the input.
func DeepCloneRefOfConvertExpr(n *ConvertExpr) *ConvertExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Expr = DeepCloneExpr(n.Expr)
	out.Type = DeepCloneRefOfConvertType(n.Type)
	return &out
}

// CloneRefOfConvertType creates a deep clone of the input.
func DeepCloneRefOfConvertType(n *ConvertType) *ConvertType {
	if n == nil {
		return nil
	}
	out := *n
	out.Length = DeepCloneRefOfLiteral(n.Length)
	out.Scale = DeepCloneRefOfLiteral(n.Scale)
	out.Charset = DeepCloneColumnCharset(n.Charset)
	return &out
}

// CloneRefOfConvertUsingExpr creates a deep clone of the input.
func DeepCloneRefOfConvertUsingExpr(n *ConvertUsingExpr) *ConvertUsingExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Expr = DeepCloneExpr(n.Expr)
	return &out
}

// CloneRefOfCount creates a deep clone of the input.
func DeepCloneRefOfCount(n *Count) *Count {
	if n == nil {
		return nil
	}
	out := *n
	out.Args = DeepCloneExprs(n.Args)
	return &out
}

// CloneRefOfCountStar creates a deep clone of the input.
func DeepCloneRefOfCountStar(n *CountStar) *CountStar {
	if n == nil {
		return nil
	}
	out := *n
	return &out
}

// CloneRefOfCreateDatabase creates a deep clone of the input.
func DeepCloneRefOfCreateDatabase(n *CreateDatabase) *CreateDatabase {
	if n == nil {
		return nil
	}
	out := *n
	out.Comments = DeepCloneRefOfParsedComments(n.Comments)
	out.DBName = DeepCloneIdentifierCS(n.DBName)
	out.CreateOptions = DeepCloneSliceOfDatabaseOption(n.CreateOptions)
	return &out
}

// CloneRefOfCreateTable creates a deep clone of the input.
func DeepCloneRefOfCreateTable(n *CreateTable) *CreateTable {
	if n == nil {
		return nil
	}
	out := *n
	out.Table = DeepCloneTableName(n.Table)
	out.TableSpec = DeepCloneRefOfTableSpec(n.TableSpec)
	out.OptLike = DeepCloneRefOfOptLike(n.OptLike)
	out.Comments = DeepCloneRefOfParsedComments(n.Comments)
	return &out
}

// CloneRefOfCreateView creates a deep clone of the input.
func DeepCloneRefOfCreateView(n *CreateView) *CreateView {
	if n == nil {
		return nil
	}
	out := *n
	out.ViewName = DeepCloneTableName(n.ViewName)
	out.Definer = DeepCloneRefOfDefiner(n.Definer)
	out.Columns = DeepCloneColumns(n.Columns)
	out.Select = DeepCloneSelectStatement(n.Select)
	out.Comments = DeepCloneRefOfParsedComments(n.Comments)
	return &out
}

// CloneRefOfCurTimeFuncExpr creates a deep clone of the input.
func DeepCloneRefOfCurTimeFuncExpr(n *CurTimeFuncExpr) *CurTimeFuncExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Name = DeepCloneIdentifierCI(n.Name)
	return &out
}

// CloneRefOfDeallocateStmt creates a deep clone of the input.
func DeepCloneRefOfDeallocateStmt(n *DeallocateStmt) *DeallocateStmt {
	if n == nil {
		return nil
	}
	out := *n
	out.Comments = DeepCloneRefOfParsedComments(n.Comments)
	out.Name = DeepCloneIdentifierCI(n.Name)
	return &out
}

// CloneRefOfDefault creates a deep clone of the input.
func DeepCloneRefOfDefault(n *Default) *Default {
	if n == nil {
		return nil
	}
	out := *n
	return &out
}

// CloneRefOfDefiner creates a deep clone of the input.
func DeepCloneRefOfDefiner(n *Definer) *Definer {
	if n == nil {
		return nil
	}
	out := *n
	return &out
}

// CloneRefOfDelete creates a deep clone of the input.
func DeepCloneRefOfDelete(n *Delete) *Delete {
	if n == nil {
		return nil
	}
	out := *n
	out.With = DeepCloneRefOfWith(n.With)
	out.Comments = DeepCloneRefOfParsedComments(n.Comments)
	out.Targets = DeepCloneTableNames(n.Targets)
	out.TableExprs = DeepCloneTableExprs(n.TableExprs)
	out.Partitions = DeepClonePartitions(n.Partitions)
	out.Where = DeepCloneRefOfWhere(n.Where)
	out.OrderBy = DeepCloneOrderBy(n.OrderBy)
	out.Limit = DeepCloneRefOfLimit(n.Limit)
	return &out
}

// CloneRefOfDerivedTable creates a deep clone of the input.
func DeepCloneRefOfDerivedTable(n *DerivedTable) *DerivedTable {
	if n == nil {
		return nil
	}
	out := *n
	out.Select = DeepCloneSelectStatement(n.Select)
	return &out
}

// CloneRefOfDropColumn creates a deep clone of the input.
func DeepCloneRefOfDropColumn(n *DropColumn) *DropColumn {
	if n == nil {
		return nil
	}
	out := *n
	out.Name = DeepCloneRefOfColName(n.Name)
	return &out
}

// CloneRefOfDropDatabase creates a deep clone of the input.
func DeepCloneRefOfDropDatabase(n *DropDatabase) *DropDatabase {
	if n == nil {
		return nil
	}
	out := *n
	out.Comments = DeepCloneRefOfParsedComments(n.Comments)
	out.DBName = DeepCloneIdentifierCS(n.DBName)
	return &out
}

// CloneRefOfDropKey creates a deep clone of the input.
func DeepCloneRefOfDropKey(n *DropKey) *DropKey {
	if n == nil {
		return nil
	}
	out := *n
	out.Name = DeepCloneIdentifierCI(n.Name)
	return &out
}

// CloneRefOfDropTable creates a deep clone of the input.
func DeepCloneRefOfDropTable(n *DropTable) *DropTable {
	if n == nil {
		return nil
	}
	out := *n
	out.FromTables = DeepCloneTableNames(n.FromTables)
	out.Comments = DeepCloneRefOfParsedComments(n.Comments)
	return &out
}

// CloneRefOfDropView creates a deep clone of the input.
func DeepCloneRefOfDropView(n *DropView) *DropView {
	if n == nil {
		return nil
	}
	out := *n
	out.FromTables = DeepCloneTableNames(n.FromTables)
	out.Comments = DeepCloneRefOfParsedComments(n.Comments)
	return &out
}

// CloneRefOfExecuteStmt creates a deep clone of the input.
func DeepCloneRefOfExecuteStmt(n *ExecuteStmt) *ExecuteStmt {
	if n == nil {
		return nil
	}
	out := *n
	out.Name = DeepCloneIdentifierCI(n.Name)
	out.Comments = DeepCloneRefOfParsedComments(n.Comments)
	out.Arguments = DeepCloneSliceOfRefOfVariable(n.Arguments)
	return &out
}

// CloneRefOfExistsExpr creates a deep clone of the input.
func DeepCloneRefOfExistsExpr(n *ExistsExpr) *ExistsExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Subquery = DeepCloneRefOfSubquery(n.Subquery)
	return &out
}

// CloneRefOfExplainStmt creates a deep clone of the input.
func DeepCloneRefOfExplainStmt(n *ExplainStmt) *ExplainStmt {
	if n == nil {
		return nil
	}
	out := *n
	out.Statement = DeepCloneStatement(n.Statement)
	out.Comments = DeepCloneRefOfParsedComments(n.Comments)
	return &out
}

// CloneRefOfExplainTab creates a deep clone of the input.
func DeepCloneRefOfExplainTab(n *ExplainTab) *ExplainTab {
	if n == nil {
		return nil
	}
	out := *n
	out.Table = DeepCloneTableName(n.Table)
	return &out
}

// CloneExprs creates a deep clone of the input.
func DeepCloneExprs(n Exprs) Exprs {
	if n == nil {
		return nil
	}
	res := make(Exprs, len(n))
	for i, x := range n {
		res[i] = DeepCloneExpr(x)
	}
	return res
}

// CloneRefOfExtractFuncExpr creates a deep clone of the input.
func DeepCloneRefOfExtractFuncExpr(n *ExtractFuncExpr) *ExtractFuncExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Expr = DeepCloneExpr(n.Expr)
	return &out
}

// CloneRefOfExtractValueExpr creates a deep clone of the input.
func DeepCloneRefOfExtractValueExpr(n *ExtractValueExpr) *ExtractValueExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Fragment = DeepCloneExpr(n.Fragment)
	out.XPathExpr = DeepCloneExpr(n.XPathExpr)
	return &out
}

// CloneRefOfFieldsClause creates a deep clone of the input.
func DeepCloneRefOfFieldsClause(n *FieldsClause) *FieldsClause {
	if n == nil {
		return nil
	}
	out := *n
	out.Expr = DeepCloneExpr(n.Expr)
	return &out
}

// CloneRefOfFirstOrLastValueExpr creates a deep clone of the input.
func DeepCloneRefOfFirstOrLastValueExpr(n *FirstOrLastValueExpr) *FirstOrLastValueExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Expr = DeepCloneExpr(n.Expr)
	out.NullTreatmentClause = DeepCloneRefOfNullTreatmentClause(n.NullTreatmentClause)
	out.OverClause = DeepCloneRefOfOverClause(n.OverClause)
	return &out
}

// CloneRefOfFlush creates a deep clone of the input.
func DeepCloneRefOfFlush(n *Flush) *Flush {
	if n == nil {
		return nil
	}
	out := *n
	out.FlushOptions = DeepCloneSliceOfString(n.FlushOptions)
	out.TableNames = DeepCloneTableNames(n.TableNames)
	return &out
}

// CloneRefOfForce creates a deep clone of the input.
func DeepCloneRefOfForce(n *Force) *Force {
	if n == nil {
		return nil
	}
	out := *n
	return &out
}

// CloneRefOfForeignKeyDefinition creates a deep clone of the input.
func DeepCloneRefOfForeignKeyDefinition(n *ForeignKeyDefinition) *ForeignKeyDefinition {
	if n == nil {
		return nil
	}
	out := *n
	out.Source = DeepCloneColumns(n.Source)
	out.IndexName = DeepCloneIdentifierCI(n.IndexName)
	out.ReferenceDefinition = DeepCloneRefOfReferenceDefinition(n.ReferenceDefinition)
	return &out
}

// CloneRefOfFrameClause creates a deep clone of the input.
func DeepCloneRefOfFrameClause(n *FrameClause) *FrameClause {
	if n == nil {
		return nil
	}
	out := *n
	out.Start = DeepCloneRefOfFramePoint(n.Start)
	out.End = DeepCloneRefOfFramePoint(n.End)
	return &out
}

// CloneRefOfFramePoint creates a deep clone of the input.
func DeepCloneRefOfFramePoint(n *FramePoint) *FramePoint {
	if n == nil {
		return nil
	}
	out := *n
	out.Expr = DeepCloneExpr(n.Expr)
	return &out
}

// CloneRefOfFromFirstLastClause creates a deep clone of the input.
func DeepCloneRefOfFromFirstLastClause(n *FromFirstLastClause) *FromFirstLastClause {
	if n == nil {
		return nil
	}
	out := *n
	return &out
}

// CloneRefOfFuncExpr creates a deep clone of the input.
func DeepCloneRefOfFuncExpr(n *FuncExpr) *FuncExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Qualifier = DeepCloneIdentifierCS(n.Qualifier)
	out.Name = DeepCloneIdentifierCI(n.Name)
	out.Exprs = DeepCloneSelectExprs(n.Exprs)
	return &out
}

// CloneRefOfGTIDFuncExpr creates a deep clone of the input.
func DeepCloneRefOfGTIDFuncExpr(n *GTIDFuncExpr) *GTIDFuncExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Set1 = DeepCloneExpr(n.Set1)
	out.Set2 = DeepCloneExpr(n.Set2)
	out.Timeout = DeepCloneExpr(n.Timeout)
	out.Channel = DeepCloneExpr(n.Channel)
	return &out
}

// CloneRefOfGeoHashFromLatLongExpr creates a deep clone of the input.
func DeepCloneRefOfGeoHashFromLatLongExpr(n *GeoHashFromLatLongExpr) *GeoHashFromLatLongExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Latitude = DeepCloneExpr(n.Latitude)
	out.Longitude = DeepCloneExpr(n.Longitude)
	out.MaxLength = DeepCloneExpr(n.MaxLength)
	return &out
}

// CloneRefOfGeoHashFromPointExpr creates a deep clone of the input.
func DeepCloneRefOfGeoHashFromPointExpr(n *GeoHashFromPointExpr) *GeoHashFromPointExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Point = DeepCloneExpr(n.Point)
	out.MaxLength = DeepCloneExpr(n.MaxLength)
	return &out
}

// CloneRefOfGeoJSONFromGeomExpr creates a deep clone of the input.
func DeepCloneRefOfGeoJSONFromGeomExpr(n *GeoJSONFromGeomExpr) *GeoJSONFromGeomExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Geom = DeepCloneExpr(n.Geom)
	out.MaxDecimalDigits = DeepCloneExpr(n.MaxDecimalDigits)
	out.Bitmask = DeepCloneExpr(n.Bitmask)
	return &out
}

// CloneRefOfGeomCollPropertyFuncExpr creates a deep clone of the input.
func DeepCloneRefOfGeomCollPropertyFuncExpr(n *GeomCollPropertyFuncExpr) *GeomCollPropertyFuncExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.GeomColl = DeepCloneExpr(n.GeomColl)
	out.PropertyDefArg = DeepCloneExpr(n.PropertyDefArg)
	return &out
}

// CloneRefOfGeomFormatExpr creates a deep clone of the input.
func DeepCloneRefOfGeomFormatExpr(n *GeomFormatExpr) *GeomFormatExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Geom = DeepCloneExpr(n.Geom)
	out.AxisOrderOpt = DeepCloneExpr(n.AxisOrderOpt)
	return &out
}

// CloneRefOfGeomFromGeoHashExpr creates a deep clone of the input.
func DeepCloneRefOfGeomFromGeoHashExpr(n *GeomFromGeoHashExpr) *GeomFromGeoHashExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.GeoHash = DeepCloneExpr(n.GeoHash)
	out.SridOpt = DeepCloneExpr(n.SridOpt)
	return &out
}

// CloneRefOfGeomFromGeoJSONExpr creates a deep clone of the input.
func DeepCloneRefOfGeomFromGeoJSONExpr(n *GeomFromGeoJSONExpr) *GeomFromGeoJSONExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.GeoJSON = DeepCloneExpr(n.GeoJSON)
	out.HigherDimHandlerOpt = DeepCloneExpr(n.HigherDimHandlerOpt)
	out.Srid = DeepCloneExpr(n.Srid)
	return &out
}

// CloneRefOfGeomFromTextExpr creates a deep clone of the input.
func DeepCloneRefOfGeomFromTextExpr(n *GeomFromTextExpr) *GeomFromTextExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.WktText = DeepCloneExpr(n.WktText)
	out.Srid = DeepCloneExpr(n.Srid)
	out.AxisOrderOpt = DeepCloneExpr(n.AxisOrderOpt)
	return &out
}

// CloneRefOfGeomFromWKBExpr creates a deep clone of the input.
func DeepCloneRefOfGeomFromWKBExpr(n *GeomFromWKBExpr) *GeomFromWKBExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.WkbBlob = DeepCloneExpr(n.WkbBlob)
	out.Srid = DeepCloneExpr(n.Srid)
	out.AxisOrderOpt = DeepCloneExpr(n.AxisOrderOpt)
	return &out
}

// CloneRefOfGeomPropertyFuncExpr creates a deep clone of the input.
func DeepCloneRefOfGeomPropertyFuncExpr(n *GeomPropertyFuncExpr) *GeomPropertyFuncExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Geom = DeepCloneExpr(n.Geom)
	return &out
}

// CloneGroupBy creates a deep clone of the input.
func DeepCloneGroupBy(n GroupBy) GroupBy {
	if n == nil {
		return nil
	}
	res := make(GroupBy, len(n))
	for i, x := range n {
		res[i] = DeepCloneExpr(x)
	}
	return res
}

// CloneRefOfGroupConcatExpr creates a deep clone of the input.
func DeepCloneRefOfGroupConcatExpr(n *GroupConcatExpr) *GroupConcatExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Exprs = DeepCloneExprs(n.Exprs)
	out.OrderBy = DeepCloneOrderBy(n.OrderBy)
	out.Limit = DeepCloneRefOfLimit(n.Limit)
	return &out
}

// CloneIdentifierCI creates a deep clone of the input.
func DeepCloneIdentifierCI(n IdentifierCI) IdentifierCI {
	return *CloneRefOfIdentifierCI(&n)
}

// CloneIdentifierCS creates a deep clone of the input.
func DeepCloneIdentifierCS(n IdentifierCS) IdentifierCS {
	return *CloneRefOfIdentifierCS(&n)
}

// CloneRefOfIndexDefinition creates a deep clone of the input.
func DeepCloneRefOfIndexDefinition(n *IndexDefinition) *IndexDefinition {
	if n == nil {
		return nil
	}
	out := *n
	out.Info = DeepCloneRefOfIndexInfo(n.Info)
	out.Columns = DeepCloneSliceOfRefOfIndexColumn(n.Columns)
	out.Options = DeepCloneSliceOfRefOfIndexOption(n.Options)
	return &out
}

// CloneRefOfIndexHint creates a deep clone of the input.
func DeepCloneRefOfIndexHint(n *IndexHint) *IndexHint {
	if n == nil {
		return nil
	}
	out := *n
	out.Indexes = DeepCloneSliceOfIdentifierCI(n.Indexes)
	return &out
}

// CloneIndexHints creates a deep clone of the input.
func DeepCloneIndexHints(n IndexHints) IndexHints {
	if n == nil {
		return nil
	}
	res := make(IndexHints, len(n))
	for i, x := range n {
		res[i] = DeepCloneRefOfIndexHint(x)
	}
	return res
}

// CloneRefOfIndexInfo creates a deep clone of the input.
func DeepCloneRefOfIndexInfo(n *IndexInfo) *IndexInfo {
	if n == nil {
		return nil
	}
	out := *n
	out.Name = DeepCloneIdentifierCI(n.Name)
	out.ConstraintName = DeepCloneIdentifierCI(n.ConstraintName)
	return &out
}

// CloneRefOfInsert creates a deep clone of the input.
func DeepCloneRefOfInsert(n *Insert) *Insert {
	if n == nil {
		return nil
	}
	out := *n
	out.Comments = DeepCloneRefOfParsedComments(n.Comments)
	out.Table = DeepCloneRefOfAliasedTableExpr(n.Table)
	out.Partitions = DeepClonePartitions(n.Partitions)
	out.Columns = DeepCloneColumns(n.Columns)
	out.Rows = DeepCloneInsertRows(n.Rows)
	out.OnDup = DeepCloneOnDup(n.OnDup)
	return &out
}

// CloneRefOfInsertExpr creates a deep clone of the input.
func DeepCloneRefOfInsertExpr(n *InsertExpr) *InsertExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Str = DeepCloneExpr(n.Str)
	out.Pos = DeepCloneExpr(n.Pos)
	out.Len = DeepCloneExpr(n.Len)
	out.NewStr = DeepCloneExpr(n.NewStr)
	return &out
}

// CloneRefOfIntervalDateExpr creates a deep clone of the input.
func DeepCloneRefOfIntervalDateExpr(n *IntervalDateExpr) *IntervalDateExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Date = DeepCloneExpr(n.Date)
	out.Interval = DeepCloneExpr(n.Interval)
	return &out
}

// CloneRefOfIntervalFuncExpr creates a deep clone of the input.
func DeepCloneRefOfIntervalFuncExpr(n *IntervalFuncExpr) *IntervalFuncExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Expr = DeepCloneExpr(n.Expr)
	out.Exprs = DeepCloneExprs(n.Exprs)
	return &out
}

// CloneRefOfIntroducerExpr creates a deep clone of the input.
func DeepCloneRefOfIntroducerExpr(n *IntroducerExpr) *IntroducerExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Expr = DeepCloneExpr(n.Expr)
	return &out
}

// CloneRefOfIsExpr creates a deep clone of the input.
func DeepCloneRefOfIsExpr(n *IsExpr) *IsExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Left = DeepCloneExpr(n.Left)
	return &out
}

// CloneRefOfJSONArrayExpr creates a deep clone of the input.
func DeepCloneRefOfJSONArrayExpr(n *JSONArrayExpr) *JSONArrayExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Params = DeepCloneExprs(n.Params)
	return &out
}

// CloneRefOfJSONAttributesExpr creates a deep clone of the input.
func DeepCloneRefOfJSONAttributesExpr(n *JSONAttributesExpr) *JSONAttributesExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.JSONDoc = DeepCloneExpr(n.JSONDoc)
	out.Path = DeepCloneExpr(n.Path)
	return &out
}

// CloneRefOfJSONContainsExpr creates a deep clone of the input.
func DeepCloneRefOfJSONContainsExpr(n *JSONContainsExpr) *JSONContainsExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Target = DeepCloneExpr(n.Target)
	out.Candidate = DeepCloneExpr(n.Candidate)
	out.PathList = DeepCloneSliceOfExpr(n.PathList)
	return &out
}

// CloneRefOfJSONContainsPathExpr creates a deep clone of the input.
func DeepCloneRefOfJSONContainsPathExpr(n *JSONContainsPathExpr) *JSONContainsPathExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.JSONDoc = DeepCloneExpr(n.JSONDoc)
	out.OneOrAll = DeepCloneExpr(n.OneOrAll)
	out.PathList = DeepCloneSliceOfExpr(n.PathList)
	return &out
}

// CloneRefOfJSONExtractExpr creates a deep clone of the input.
func DeepCloneRefOfJSONExtractExpr(n *JSONExtractExpr) *JSONExtractExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.JSONDoc = DeepCloneExpr(n.JSONDoc)
	out.PathList = DeepCloneSliceOfExpr(n.PathList)
	return &out
}

// CloneRefOfJSONKeysExpr creates a deep clone of the input.
func DeepCloneRefOfJSONKeysExpr(n *JSONKeysExpr) *JSONKeysExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.JSONDoc = DeepCloneExpr(n.JSONDoc)
	out.Path = DeepCloneExpr(n.Path)
	return &out
}

// CloneRefOfJSONObjectExpr creates a deep clone of the input.
func DeepCloneRefOfJSONObjectExpr(n *JSONObjectExpr) *JSONObjectExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Params = DeepCloneSliceOfRefOfJSONObjectParam(n.Params)
	return &out
}

// CloneRefOfJSONObjectParam creates a deep clone of the input.
func DeepCloneRefOfJSONObjectParam(n *JSONObjectParam) *JSONObjectParam {
	if n == nil {
		return nil
	}
	out := *n
	out.Key = DeepCloneExpr(n.Key)
	out.Value = DeepCloneExpr(n.Value)
	return &out
}

// CloneRefOfJSONOverlapsExpr creates a deep clone of the input.
func DeepCloneRefOfJSONOverlapsExpr(n *JSONOverlapsExpr) *JSONOverlapsExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.JSONDoc1 = DeepCloneExpr(n.JSONDoc1)
	out.JSONDoc2 = DeepCloneExpr(n.JSONDoc2)
	return &out
}

// CloneRefOfJSONPrettyExpr creates a deep clone of the input.
func DeepCloneRefOfJSONPrettyExpr(n *JSONPrettyExpr) *JSONPrettyExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.JSONVal = DeepCloneExpr(n.JSONVal)
	return &out
}

// CloneRefOfJSONQuoteExpr creates a deep clone of the input.
func DeepCloneRefOfJSONQuoteExpr(n *JSONQuoteExpr) *JSONQuoteExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.StringArg = DeepCloneExpr(n.StringArg)
	return &out
}

// CloneRefOfJSONRemoveExpr creates a deep clone of the input.
func DeepCloneRefOfJSONRemoveExpr(n *JSONRemoveExpr) *JSONRemoveExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.JSONDoc = DeepCloneExpr(n.JSONDoc)
	out.PathList = DeepCloneExprs(n.PathList)
	return &out
}

// CloneRefOfJSONSchemaValidFuncExpr creates a deep clone of the input.
func DeepCloneRefOfJSONSchemaValidFuncExpr(n *JSONSchemaValidFuncExpr) *JSONSchemaValidFuncExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Schema = DeepCloneExpr(n.Schema)
	out.Document = DeepCloneExpr(n.Document)
	return &out
}

// CloneRefOfJSONSchemaValidationReportFuncExpr creates a deep clone of the input.
func DeepCloneRefOfJSONSchemaValidationReportFuncExpr(n *JSONSchemaValidationReportFuncExpr) *JSONSchemaValidationReportFuncExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Schema = DeepCloneExpr(n.Schema)
	out.Document = DeepCloneExpr(n.Document)
	return &out
}

// CloneRefOfJSONSearchExpr creates a deep clone of the input.
func DeepCloneRefOfJSONSearchExpr(n *JSONSearchExpr) *JSONSearchExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.JSONDoc = DeepCloneExpr(n.JSONDoc)
	out.OneOrAll = DeepCloneExpr(n.OneOrAll)
	out.SearchStr = DeepCloneExpr(n.SearchStr)
	out.EscapeChar = DeepCloneExpr(n.EscapeChar)
	out.PathList = DeepCloneSliceOfExpr(n.PathList)
	return &out
}

// CloneRefOfJSONStorageFreeExpr creates a deep clone of the input.
func DeepCloneRefOfJSONStorageFreeExpr(n *JSONStorageFreeExpr) *JSONStorageFreeExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.JSONVal = DeepCloneExpr(n.JSONVal)
	return &out
}

// CloneRefOfJSONStorageSizeExpr creates a deep clone of the input.
func DeepCloneRefOfJSONStorageSizeExpr(n *JSONStorageSizeExpr) *JSONStorageSizeExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.JSONVal = DeepCloneExpr(n.JSONVal)
	return &out
}

// CloneRefOfJSONTableExpr creates a deep clone of the input.
func DeepCloneRefOfJSONTableExpr(n *JSONTableExpr) *JSONTableExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Expr = DeepCloneExpr(n.Expr)
	out.Alias = DeepCloneIdentifierCS(n.Alias)
	out.Filter = DeepCloneExpr(n.Filter)
	out.Columns = DeepCloneSliceOfRefOfJtColumnDefinition(n.Columns)
	return &out
}

// CloneRefOfJSONUnquoteExpr creates a deep clone of the input.
func DeepCloneRefOfJSONUnquoteExpr(n *JSONUnquoteExpr) *JSONUnquoteExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.JSONValue = DeepCloneExpr(n.JSONValue)
	return &out
}

// CloneRefOfJSONValueExpr creates a deep clone of the input.
func DeepCloneRefOfJSONValueExpr(n *JSONValueExpr) *JSONValueExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.JSONDoc = DeepCloneExpr(n.JSONDoc)
	out.Path = DeepCloneExpr(n.Path)
	out.ReturningType = DeepCloneRefOfConvertType(n.ReturningType)
	out.EmptyOnResponse = DeepCloneRefOfJtOnResponse(n.EmptyOnResponse)
	out.ErrorOnResponse = DeepCloneRefOfJtOnResponse(n.ErrorOnResponse)
	return &out
}

// CloneRefOfJSONValueMergeExpr creates a deep clone of the input.
func DeepCloneRefOfJSONValueMergeExpr(n *JSONValueMergeExpr) *JSONValueMergeExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.JSONDoc = DeepCloneExpr(n.JSONDoc)
	out.JSONDocList = DeepCloneExprs(n.JSONDocList)
	return &out
}

// CloneRefOfJSONValueModifierExpr creates a deep clone of the input.
func DeepCloneRefOfJSONValueModifierExpr(n *JSONValueModifierExpr) *JSONValueModifierExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.JSONDoc = DeepCloneExpr(n.JSONDoc)
	out.Params = DeepCloneSliceOfRefOfJSONObjectParam(n.Params)
	return &out
}

// CloneRefOfJoinCondition creates a deep clone of the input.
func DeepCloneRefOfJoinCondition(n *JoinCondition) *JoinCondition {
	if n == nil {
		return nil
	}
	out := *n
	out.On = DeepCloneExpr(n.On)
	out.Using = DeepCloneColumns(n.Using)
	return &out
}

// CloneRefOfJoinTableExpr creates a deep clone of the input.
func DeepCloneRefOfJoinTableExpr(n *JoinTableExpr) *JoinTableExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.LeftExpr = DeepCloneTableExpr(n.LeftExpr)
	out.RightExpr = DeepCloneTableExpr(n.RightExpr)
	out.Condition = DeepCloneRefOfJoinCondition(n.Condition)
	return &out
}

// CloneRefOfJtColumnDefinition creates a deep clone of the input.
func DeepCloneRefOfJtColumnDefinition(n *JtColumnDefinition) *JtColumnDefinition {
	if n == nil {
		return nil
	}
	out := *n
	out.JtOrdinal = DeepCloneRefOfJtOrdinalColDef(n.JtOrdinal)
	out.JtPath = DeepCloneRefOfJtPathColDef(n.JtPath)
	out.JtNestedPath = DeepCloneRefOfJtNestedPathColDef(n.JtNestedPath)
	return &out
}

// CloneRefOfJtOnResponse creates a deep clone of the input.
func DeepCloneRefOfJtOnResponse(n *JtOnResponse) *JtOnResponse {
	if n == nil {
		return nil
	}
	out := *n
	out.Expr = DeepCloneExpr(n.Expr)
	return &out
}

// CloneRefOfKeyState creates a deep clone of the input.
func DeepCloneRefOfKeyState(n *KeyState) *KeyState {
	if n == nil {
		return nil
	}
	out := *n
	return &out
}

// CloneRefOfKill creates a deep clone of the input.
func DeepCloneRefOfKill(n *Kill) *Kill {
	if n == nil {
		return nil
	}
	out := *n
	return &out
}

// CloneRefOfLagLeadExpr creates a deep clone of the input.
func DeepCloneRefOfLagLeadExpr(n *LagLeadExpr) *LagLeadExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Expr = DeepCloneExpr(n.Expr)
	out.N = DeepCloneExpr(n.N)
	out.Default = DeepCloneExpr(n.Default)
	out.OverClause = DeepCloneRefOfOverClause(n.OverClause)
	out.NullTreatmentClause = DeepCloneRefOfNullTreatmentClause(n.NullTreatmentClause)
	return &out
}

// CloneRefOfLimit creates a deep clone of the input.
func DeepCloneRefOfLimit(n *Limit) *Limit {
	if n == nil {
		return nil
	}
	out := *n
	out.Offset = DeepCloneExpr(n.Offset)
	out.Rowcount = DeepCloneExpr(n.Rowcount)
	return &out
}

// CloneRefOfLineStringExpr creates a deep clone of the input.
func DeepCloneRefOfLineStringExpr(n *LineStringExpr) *LineStringExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.PointParams = DeepCloneExprs(n.PointParams)
	return &out
}

// CloneRefOfLinesClause creates a deep clone of the input.
func DeepCloneRefOfLinesClause(n *LinesClause) *LinesClause {
	if n == nil {
		return nil
	}
	out := *n
	out.Expr = DeepCloneExpr(n.Expr)
	return &out
}

// CloneRefOfLinestrPropertyFuncExpr creates a deep clone of the input.
func DeepCloneRefOfLinestrPropertyFuncExpr(n *LinestrPropertyFuncExpr) *LinestrPropertyFuncExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Linestring = DeepCloneExpr(n.Linestring)
	out.PropertyDefArg = DeepCloneExpr(n.PropertyDefArg)
	return &out
}

// CloneRefOfLiteral creates a deep clone of the input.
func DeepCloneRefOfLiteral(n *Literal) *Literal {
	if n == nil {
		return nil
	}
	out := *n
	return &out
}

// CloneRefOfLoad creates a deep clone of the input.
func DeepCloneRefOfLoad(n *Load) *Load {
	if n == nil {
		return nil
	}
	out := *n
	return &out
}

// CloneRefOfLoadDataStmt creates a deep clone of the input.
func DeepCloneRefOfLoadDataStmt(n *LoadDataStmt) *LoadDataStmt {
	if n == nil {
		return nil
	}
	out := *n
	out.Table = DeepCloneTableName(n.Table)
	out.Columns = DeepCloneColumns(n.Columns)
	out.FieldsInfo = DeepCloneRefOfFieldsClause(n.FieldsInfo)
	out.LinesInfo = DeepCloneRefOfLinesClause(n.LinesInfo)
	return &out
}

// CloneRefOfLocateExpr creates a deep clone of the input.
func DeepCloneRefOfLocateExpr(n *LocateExpr) *LocateExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.SubStr = DeepCloneExpr(n.SubStr)
	out.Str = DeepCloneExpr(n.Str)
	out.Pos = DeepCloneExpr(n.Pos)
	return &out
}

// CloneRefOfLockOption creates a deep clone of the input.
func DeepCloneRefOfLockOption(n *LockOption) *LockOption {
	if n == nil {
		return nil
	}
	out := *n
	return &out
}

// CloneRefOfLockTables creates a deep clone of the input.
func DeepCloneRefOfLockTables(n *LockTables) *LockTables {
	if n == nil {
		return nil
	}
	out := *n
	out.Tables = DeepCloneTableAndLockTypes(n.Tables)
	return &out
}

// CloneRefOfLockingFunc creates a deep clone of the input.
func DeepCloneRefOfLockingFunc(n *LockingFunc) *LockingFunc {
	if n == nil {
		return nil
	}
	out := *n
	out.Name = DeepCloneExpr(n.Name)
	out.Timeout = DeepCloneExpr(n.Timeout)
	return &out
}

// CloneRefOfMatchExpr creates a deep clone of the input.
func DeepCloneRefOfMatchExpr(n *MatchExpr) *MatchExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Columns = DeepCloneSliceOfRefOfColName(n.Columns)
	out.Expr = DeepCloneExpr(n.Expr)
	return &out
}

// CloneRefOfMax creates a deep clone of the input.
func DeepCloneRefOfMax(n *Max) *Max {
	if n == nil {
		return nil
	}
	out := *n
	out.Arg = DeepCloneExpr(n.Arg)
	return &out
}

// CloneRefOfMemberOfExpr creates a deep clone of the input.
func DeepCloneRefOfMemberOfExpr(n *MemberOfExpr) *MemberOfExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Value = DeepCloneExpr(n.Value)
	out.JSONArr = DeepCloneExpr(n.JSONArr)
	return &out
}

// CloneRefOfMin creates a deep clone of the input.
func DeepCloneRefOfMin(n *Min) *Min {
	if n == nil {
		return nil
	}
	out := *n
	out.Arg = DeepCloneExpr(n.Arg)
	return &out
}

// CloneRefOfModifyColumn creates a deep clone of the input.
func DeepCloneRefOfModifyColumn(n *ModifyColumn) *ModifyColumn {
	if n == nil {
		return nil
	}
	out := *n
	out.NewColDefinition = DeepCloneRefOfColumnDefinition(n.NewColDefinition)
	out.After = DeepCloneRefOfColName(n.After)
	return &out
}

// CloneRefOfMultiLinestringExpr creates a deep clone of the input.
func DeepCloneRefOfMultiLinestringExpr(n *MultiLinestringExpr) *MultiLinestringExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.LinestringParams = DeepCloneExprs(n.LinestringParams)
	return &out
}

// CloneRefOfMultiPointExpr creates a deep clone of the input.
func DeepCloneRefOfMultiPointExpr(n *MultiPointExpr) *MultiPointExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.PointParams = DeepCloneExprs(n.PointParams)
	return &out
}

// CloneRefOfMultiPolygonExpr creates a deep clone of the input.
func DeepCloneRefOfMultiPolygonExpr(n *MultiPolygonExpr) *MultiPolygonExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.PolygonParams = DeepCloneExprs(n.PolygonParams)
	return &out
}

// CloneRefOfNTHValueExpr creates a deep clone of the input.
func DeepCloneRefOfNTHValueExpr(n *NTHValueExpr) *NTHValueExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Expr = DeepCloneExpr(n.Expr)
	out.N = DeepCloneExpr(n.N)
	out.OverClause = DeepCloneRefOfOverClause(n.OverClause)
	out.FromFirstLastClause = DeepCloneRefOfFromFirstLastClause(n.FromFirstLastClause)
	out.NullTreatmentClause = DeepCloneRefOfNullTreatmentClause(n.NullTreatmentClause)
	return &out
}

// CloneRefOfNamedWindow creates a deep clone of the input.
func DeepCloneRefOfNamedWindow(n *NamedWindow) *NamedWindow {
	if n == nil {
		return nil
	}
	out := *n
	out.Windows = DeepCloneWindowDefinitions(n.Windows)
	return &out
}

// CloneNamedWindows creates a deep clone of the input.
func DeepCloneNamedWindows(n NamedWindows) NamedWindows {
	if n == nil {
		return nil
	}
	res := make(NamedWindows, len(n))
	for i, x := range n {
		res[i] = DeepCloneRefOfNamedWindow(x)
	}
	return res
}

// CloneRefOfNextval creates a deep clone of the input.
func DeepCloneRefOfNextval(n *Nextval) *Nextval {
	if n == nil {
		return nil
	}
	out := *n
	out.Expr = DeepCloneExpr(n.Expr)
	return &out
}

// CloneRefOfNotExpr creates a deep clone of the input.
func DeepCloneRefOfNotExpr(n *NotExpr) *NotExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Expr = DeepCloneExpr(n.Expr)
	return &out
}

// CloneRefOfNtileExpr creates a deep clone of the input.
func DeepCloneRefOfNtileExpr(n *NtileExpr) *NtileExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.N = DeepCloneExpr(n.N)
	out.OverClause = DeepCloneRefOfOverClause(n.OverClause)
	return &out
}

// CloneRefOfNullTreatmentClause creates a deep clone of the input.
func DeepCloneRefOfNullTreatmentClause(n *NullTreatmentClause) *NullTreatmentClause {
	if n == nil {
		return nil
	}
	out := *n
	return &out
}

// CloneRefOfNullVal creates a deep clone of the input.
func DeepCloneRefOfNullVal(n *NullVal) *NullVal {
	if n == nil {
		return nil
	}
	out := *n
	return &out
}

// CloneRefOfOffset creates a deep clone of the input.
func DeepCloneRefOfOffset(n *Offset) *Offset {
	if n == nil {
		return nil
	}
	out := *n
	out.Original = DeepCloneExpr(n.Original)
	return &out
}

// CloneOnDup creates a deep clone of the input.
func DeepCloneOnDup(n OnDup) OnDup {
	if n == nil {
		return nil
	}
	res := make(OnDup, len(n))
	for i, x := range n {
		res[i] = DeepCloneRefOfUpdateExpr(x)
	}
	return res
}

// CloneRefOfOptLike creates a deep clone of the input.
func DeepCloneRefOfOptLike(n *OptLike) *OptLike {
	if n == nil {
		return nil
	}
	out := *n
	out.LikeTable = DeepCloneTableName(n.LikeTable)
	return &out
}

// CloneRefOfOrExpr creates a deep clone of the input.
func DeepCloneRefOfOrExpr(n *OrExpr) *OrExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Left = DeepCloneExpr(n.Left)
	out.Right = DeepCloneExpr(n.Right)
	return &out
}

// CloneRefOfOrder creates a deep clone of the input.
func DeepCloneRefOfOrder(n *Order) *Order {
	if n == nil {
		return nil
	}
	out := *n
	out.Expr = DeepCloneExpr(n.Expr)
	return &out
}

// CloneOrderBy creates a deep clone of the input.
func DeepCloneOrderBy(n OrderBy) OrderBy {
	if n == nil {
		return nil
	}
	res := make(OrderBy, len(n))
	for i, x := range n {
		res[i] = DeepCloneRefOfOrder(x)
	}
	return res
}

// CloneRefOfOrderByOption creates a deep clone of the input.
func DeepCloneRefOfOrderByOption(n *OrderByOption) *OrderByOption {
	if n == nil {
		return nil
	}
	out := *n
	out.Cols = DeepCloneColumns(n.Cols)
	return &out
}

// CloneRefOfOtherAdmin creates a deep clone of the input.
func DeepCloneRefOfOtherAdmin(n *OtherAdmin) *OtherAdmin {
	if n == nil {
		return nil
	}
	out := *n
	return &out
}

// CloneRefOfOverClause creates a deep clone of the input.
func DeepCloneRefOfOverClause(n *OverClause) *OverClause {
	if n == nil {
		return nil
	}
	out := *n
	out.WindowName = DeepCloneIdentifierCI(n.WindowName)
	out.WindowSpec = DeepCloneRefOfWindowSpecification(n.WindowSpec)
	return &out
}

// CloneRefOfParenTableExpr creates a deep clone of the input.
func DeepCloneRefOfParenTableExpr(n *ParenTableExpr) *ParenTableExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Exprs = DeepCloneTableExprs(n.Exprs)
	return &out
}

// CloneRefOfParsedComments creates a deep clone of the input.
func DeepCloneRefOfParsedComments(n *ParsedComments) *ParsedComments {
	if n == nil {
		return nil
	}
	out := *n
	out.comments = DeepCloneComments(n.comments)
	return &out
}

// CloneRefOfPartitionDefinition creates a deep clone of the input.
func DeepCloneRefOfPartitionDefinition(n *PartitionDefinition) *PartitionDefinition {
	if n == nil {
		return nil
	}
	out := *n
	out.Name = DeepCloneIdentifierCI(n.Name)
	out.Options = DeepCloneRefOfPartitionDefinitionOptions(n.Options)
	return &out
}

// CloneRefOfPartitionDefinitionOptions creates a deep clone of the input.
func DeepCloneRefOfPartitionDefinitionOptions(n *PartitionDefinitionOptions) *PartitionDefinitionOptions {
	if n == nil {
		return nil
	}
	out := *n
	out.ValueRange = DeepCloneRefOfPartitionValueRange(n.ValueRange)
	out.Comment = DeepCloneRefOfLiteral(n.Comment)
	out.Engine = DeepCloneRefOfPartitionEngine(n.Engine)
	out.DataDirectory = DeepCloneRefOfLiteral(n.DataDirectory)
	out.IndexDirectory = DeepCloneRefOfLiteral(n.IndexDirectory)
	out.MaxRows = DeepCloneRefOfInt(n.MaxRows)
	out.MinRows = DeepCloneRefOfInt(n.MinRows)
	out.SubPartitionDefinitions = DeepCloneSubPartitionDefinitions(n.SubPartitionDefinitions)
	return &out
}

// CloneRefOfPartitionEngine creates a deep clone of the input.
func DeepCloneRefOfPartitionEngine(n *PartitionEngine) *PartitionEngine {
	if n == nil {
		return nil
	}
	out := *n
	return &out
}

// CloneRefOfPartitionOption creates a deep clone of the input.
func DeepCloneRefOfPartitionOption(n *PartitionOption) *PartitionOption {
	if n == nil {
		return nil
	}
	out := *n
	out.ColList = DeepCloneColumns(n.ColList)
	out.Expr = DeepCloneExpr(n.Expr)
	out.SubPartition = DeepCloneRefOfSubPartition(n.SubPartition)
	out.Definitions = DeepCloneSliceOfRefOfPartitionDefinition(n.Definitions)
	return &out
}

// CloneRefOfPartitionSpec creates a deep clone of the input.
func DeepCloneRefOfPartitionSpec(n *PartitionSpec) *PartitionSpec {
	if n == nil {
		return nil
	}
	out := *n
	out.Names = DeepClonePartitions(n.Names)
	out.Number = DeepCloneRefOfLiteral(n.Number)
	out.TableName = DeepCloneTableName(n.TableName)
	out.Definitions = DeepCloneSliceOfRefOfPartitionDefinition(n.Definitions)
	return &out
}

// CloneRefOfPartitionValueRange creates a deep clone of the input.
func DeepCloneRefOfPartitionValueRange(n *PartitionValueRange) *PartitionValueRange {
	if n == nil {
		return nil
	}
	out := *n
	out.Range = DeepCloneValTuple(n.Range)
	return &out
}

// ClonePartitions creates a deep clone of the input.
func DeepClonePartitions(n Partitions) Partitions {
	if n == nil {
		return nil
	}
	res := make(Partitions, len(n))
	for i, x := range n {
		res[i] = DeepCloneIdentifierCI(x)
	}
	return res
}

// CloneRefOfPerformanceSchemaFuncExpr creates a deep clone of the input.
func DeepCloneRefOfPerformanceSchemaFuncExpr(n *PerformanceSchemaFuncExpr) *PerformanceSchemaFuncExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Argument = DeepCloneExpr(n.Argument)
	return &out
}

// CloneRefOfPointExpr creates a deep clone of the input.
func DeepCloneRefOfPointExpr(n *PointExpr) *PointExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.XCordinate = DeepCloneExpr(n.XCordinate)
	out.YCordinate = DeepCloneExpr(n.YCordinate)
	return &out
}

// CloneRefOfPointPropertyFuncExpr creates a deep clone of the input.
func DeepCloneRefOfPointPropertyFuncExpr(n *PointPropertyFuncExpr) *PointPropertyFuncExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Point = DeepCloneExpr(n.Point)
	out.ValueToSet = DeepCloneExpr(n.ValueToSet)
	return &out
}

// CloneRefOfPolygonExpr creates a deep clone of the input.
func DeepCloneRefOfPolygonExpr(n *PolygonExpr) *PolygonExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.LinestringParams = DeepCloneExprs(n.LinestringParams)
	return &out
}

// CloneRefOfPolygonPropertyFuncExpr creates a deep clone of the input.
func DeepCloneRefOfPolygonPropertyFuncExpr(n *PolygonPropertyFuncExpr) *PolygonPropertyFuncExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Polygon = DeepCloneExpr(n.Polygon)
	out.PropertyDefArg = DeepCloneExpr(n.PropertyDefArg)
	return &out
}

// CloneRefOfPrepareStmt creates a deep clone of the input.
func DeepCloneRefOfPrepareStmt(n *PrepareStmt) *PrepareStmt {
	if n == nil {
		return nil
	}
	out := *n
	out.Name = DeepCloneIdentifierCI(n.Name)
	out.Statement = DeepCloneExpr(n.Statement)
	out.Comments = DeepCloneRefOfParsedComments(n.Comments)
	return &out
}

// CloneRefOfPurgeBinaryLogs creates a deep clone of the input.
func DeepCloneRefOfPurgeBinaryLogs(n *PurgeBinaryLogs) *PurgeBinaryLogs {
	if n == nil {
		return nil
	}
	out := *n
	return &out
}

// CloneRefOfReferenceDefinition creates a deep clone of the input.
func DeepCloneRefOfReferenceDefinition(n *ReferenceDefinition) *ReferenceDefinition {
	if n == nil {
		return nil
	}
	out := *n
	out.ReferencedTable = DeepCloneTableName(n.ReferencedTable)
	out.ReferencedColumns = DeepCloneColumns(n.ReferencedColumns)
	return &out
}

// CloneRefOfRegexpInstrExpr creates a deep clone of the input.
func DeepCloneRefOfRegexpInstrExpr(n *RegexpInstrExpr) *RegexpInstrExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Expr = DeepCloneExpr(n.Expr)
	out.Pattern = DeepCloneExpr(n.Pattern)
	out.Position = DeepCloneExpr(n.Position)
	out.Occurrence = DeepCloneExpr(n.Occurrence)
	out.ReturnOption = DeepCloneExpr(n.ReturnOption)
	out.MatchType = DeepCloneExpr(n.MatchType)
	return &out
}

// CloneRefOfRegexpLikeExpr creates a deep clone of the input.
func DeepCloneRefOfRegexpLikeExpr(n *RegexpLikeExpr) *RegexpLikeExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Expr = DeepCloneExpr(n.Expr)
	out.Pattern = DeepCloneExpr(n.Pattern)
	out.MatchType = DeepCloneExpr(n.MatchType)
	return &out
}

// CloneRefOfRegexpReplaceExpr creates a deep clone of the input.
func DeepCloneRefOfRegexpReplaceExpr(n *RegexpReplaceExpr) *RegexpReplaceExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Expr = DeepCloneExpr(n.Expr)
	out.Pattern = DeepCloneExpr(n.Pattern)
	out.Repl = DeepCloneExpr(n.Repl)
	out.Occurrence = DeepCloneExpr(n.Occurrence)
	out.Position = DeepCloneExpr(n.Position)
	out.MatchType = DeepCloneExpr(n.MatchType)
	return &out
}

// CloneRefOfRegexpSubstrExpr creates a deep clone of the input.
func DeepCloneRefOfRegexpSubstrExpr(n *RegexpSubstrExpr) *RegexpSubstrExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Expr = DeepCloneExpr(n.Expr)
	out.Pattern = DeepCloneExpr(n.Pattern)
	out.Occurrence = DeepCloneExpr(n.Occurrence)
	out.Position = DeepCloneExpr(n.Position)
	out.MatchType = DeepCloneExpr(n.MatchType)
	return &out
}

// CloneRefOfRelease creates a deep clone of the input.
func DeepCloneRefOfRelease(n *Release) *Release {
	if n == nil {
		return nil
	}
	out := *n
	out.Name = DeepCloneIdentifierCI(n.Name)
	return &out
}

// CloneRefOfRenameColumn creates a deep clone of the input.
func DeepCloneRefOfRenameColumn(n *RenameColumn) *RenameColumn {
	if n == nil {
		return nil
	}
	out := *n
	out.OldName = DeepCloneRefOfColName(n.OldName)
	out.NewName = DeepCloneRefOfColName(n.NewName)
	return &out
}

// CloneRefOfRenameIndex creates a deep clone of the input.
func DeepCloneRefOfRenameIndex(n *RenameIndex) *RenameIndex {
	if n == nil {
		return nil
	}
	out := *n
	out.OldName = DeepCloneIdentifierCI(n.OldName)
	out.NewName = DeepCloneIdentifierCI(n.NewName)
	return &out
}

// CloneRefOfRenameTable creates a deep clone of the input.
func DeepCloneRefOfRenameTable(n *RenameTable) *RenameTable {
	if n == nil {
		return nil
	}
	out := *n
	out.TablePairs = DeepCloneSliceOfRefOfRenameTablePair(n.TablePairs)
	return &out
}

// CloneRefOfRenameTableName creates a deep clone of the input.
func DeepCloneRefOfRenameTableName(n *RenameTableName) *RenameTableName {
	if n == nil {
		return nil
	}
	out := *n
	out.Table = DeepCloneTableName(n.Table)
	return &out
}

// CloneRefOfRevertMigration creates a deep clone of the input.
func DeepCloneRefOfRevertMigration(n *RevertMigration) *RevertMigration {
	if n == nil {
		return nil
	}
	out := *n
	out.Comments = DeepCloneRefOfParsedComments(n.Comments)
	return &out
}

// CloneRefOfRollback creates a deep clone of the input.
func DeepCloneRefOfRollback(n *Rollback) *Rollback {
	if n == nil {
		return nil
	}
	out := *n
	return &out
}

// CloneRootNode creates a deep clone of the input.
func DeepCloneRootNode(n RootNode) RootNode {
	return *CloneRefOfRootNode(&n)
}

// CloneRefOfSRollback creates a deep clone of the input.
func DeepCloneRefOfSRollback(n *SRollback) *SRollback {
	if n == nil {
		return nil
	}
	out := *n
	out.Name = DeepCloneIdentifierCI(n.Name)
	return &out
}

// CloneRefOfSavepoint creates a deep clone of the input.
func DeepCloneRefOfSavepoint(n *Savepoint) *Savepoint {
	if n == nil {
		return nil
	}
	out := *n
	out.Name = DeepCloneIdentifierCI(n.Name)
	return &out
}

// CloneRefOfSelect creates a deep clone of the input.
func DeepCloneRefOfSelect(n *Select) *Select {
	if n == nil {
		return nil
	}
	out := *n
	out.Cache = DeepCloneRefOfBool(n.Cache)
	out.From = DeepCloneSliceOfTableExpr(n.From)
	out.Comments = DeepCloneRefOfParsedComments(n.Comments)
	out.SelectExprs = DeepCloneSelectExprs(n.SelectExprs)
	out.Where = DeepCloneRefOfWhere(n.Where)
	out.With = DeepCloneRefOfWith(n.With)
	out.GroupBy = DeepCloneGroupBy(n.GroupBy)
	out.Having = DeepCloneRefOfWhere(n.Having)
	out.Windows = DeepCloneNamedWindows(n.Windows)
	out.OrderBy = DeepCloneOrderBy(n.OrderBy)
	out.Limit = DeepCloneRefOfLimit(n.Limit)
	out.Into = DeepCloneRefOfSelectInto(n.Into)
	return &out
}

// CloneSelectExprs creates a deep clone of the input.
func DeepCloneSelectExprs(n SelectExprs) SelectExprs {
	if n == nil {
		return nil
	}
	res := make(SelectExprs, len(n))
	for i, x := range n {
		res[i] = DeepCloneSelectExpr(x)
	}
	return res
}

// CloneRefOfSelectInto creates a deep clone of the input.
func DeepCloneRefOfSelectInto(n *SelectInto) *SelectInto {
	if n == nil {
		return nil
	}
	out := *n
	out.Charset = DeepCloneColumnCharset(n.Charset)
	return &out
}

// CloneRefOfSet creates a deep clone of the input.
func DeepCloneRefOfSet(n *Set) *Set {
	if n == nil {
		return nil
	}
	out := *n
	out.Comments = DeepCloneRefOfParsedComments(n.Comments)
	out.Exprs = DeepCloneSetExprs(n.Exprs)
	return &out
}

// CloneRefOfSetExpr creates a deep clone of the input.
func DeepCloneRefOfSetExpr(n *SetExpr) *SetExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Var = DeepCloneRefOfVariable(n.Var)
	out.Expr = DeepCloneExpr(n.Expr)
	return &out
}

// CloneSetExprs creates a deep clone of the input.
func DeepCloneSetExprs(n SetExprs) SetExprs {
	if n == nil {
		return nil
	}
	res := make(SetExprs, len(n))
	for i, x := range n {
		res[i] = DeepCloneRefOfSetExpr(x)
	}
	return res
}

// CloneRefOfShow creates a deep clone of the input.
func DeepCloneRefOfShow(n *Show) *Show {
	if n == nil {
		return nil
	}
	out := *n
	out.Internal = DeepCloneShowInternal(n.Internal)
	return &out
}

// CloneRefOfShowBasic creates a deep clone of the input.
func DeepCloneRefOfShowBasic(n *ShowBasic) *ShowBasic {
	if n == nil {
		return nil
	}
	out := *n
	out.Tbl = DeepCloneTableName(n.Tbl)
	out.DbName = DeepCloneIdentifierCS(n.DbName)
	out.Filter = DeepCloneRefOfShowFilter(n.Filter)
	return &out
}

// CloneRefOfShowCreate creates a deep clone of the input.
func DeepCloneRefOfShowCreate(n *ShowCreate) *ShowCreate {
	if n == nil {
		return nil
	}
	out := *n
	out.Op = DeepCloneTableName(n.Op)
	return &out
}

// CloneRefOfShowFilter creates a deep clone of the input.
func DeepCloneRefOfShowFilter(n *ShowFilter) *ShowFilter {
	if n == nil {
		return nil
	}
	out := *n
	out.Filter = DeepCloneExpr(n.Filter)
	return &out
}

// CloneRefOfShowMigrationLogs creates a deep clone of the input.
func DeepCloneRefOfShowMigrationLogs(n *ShowMigrationLogs) *ShowMigrationLogs {
	if n == nil {
		return nil
	}
	out := *n
	out.Comments = DeepCloneRefOfParsedComments(n.Comments)
	return &out
}

// CloneRefOfShowOther creates a deep clone of the input.
func DeepCloneRefOfShowOther(n *ShowOther) *ShowOther {
	if n == nil {
		return nil
	}
	out := *n
	return &out
}

// CloneRefOfShowThrottledApps creates a deep clone of the input.
func DeepCloneRefOfShowThrottledApps(n *ShowThrottledApps) *ShowThrottledApps {
	if n == nil {
		return nil
	}
	out := *n
	out.Comments = DeepCloneComments(n.Comments)
	return &out
}

// CloneRefOfShowThrottlerStatus creates a deep clone of the input.
func DeepCloneRefOfShowThrottlerStatus(n *ShowThrottlerStatus) *ShowThrottlerStatus {
	if n == nil {
		return nil
	}
	out := *n
	out.Comments = DeepCloneComments(n.Comments)
	return &out
}

// CloneRefOfStarExpr creates a deep clone of the input.
func DeepCloneRefOfStarExpr(n *StarExpr) *StarExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.TableName = DeepCloneTableName(n.TableName)
	return &out
}

// CloneRefOfStd creates a deep clone of the input.
func DeepCloneRefOfStd(n *Std) *Std {
	if n == nil {
		return nil
	}
	out := *n
	out.Arg = DeepCloneExpr(n.Arg)
	return &out
}

// CloneRefOfStdDev creates a deep clone of the input.
func DeepCloneRefOfStdDev(n *StdDev) *StdDev {
	if n == nil {
		return nil
	}
	out := *n
	out.Arg = DeepCloneExpr(n.Arg)
	return &out
}

// CloneRefOfStdPop creates a deep clone of the input.
func DeepCloneRefOfStdPop(n *StdPop) *StdPop {
	if n == nil {
		return nil
	}
	out := *n
	out.Arg = DeepCloneExpr(n.Arg)
	return &out
}

// CloneRefOfStdSamp creates a deep clone of the input.
func DeepCloneRefOfStdSamp(n *StdSamp) *StdSamp {
	if n == nil {
		return nil
	}
	out := *n
	out.Arg = DeepCloneExpr(n.Arg)
	return &out
}

// CloneRefOfStream creates a deep clone of the input.
func DeepCloneRefOfStream(n *Stream) *Stream {
	if n == nil {
		return nil
	}
	out := *n
	out.Comments = DeepCloneRefOfParsedComments(n.Comments)
	out.SelectExpr = DeepCloneSelectExpr(n.SelectExpr)
	out.Table = DeepCloneTableName(n.Table)
	return &out
}

// CloneRefOfSubPartition creates a deep clone of the input.
func DeepCloneRefOfSubPartition(n *SubPartition) *SubPartition {
	if n == nil {
		return nil
	}
	out := *n
	out.ColList = DeepCloneColumns(n.ColList)
	out.Expr = DeepCloneExpr(n.Expr)
	return &out
}

// CloneRefOfSubPartitionDefinition creates a deep clone of the input.
func DeepCloneRefOfSubPartitionDefinition(n *SubPartitionDefinition) *SubPartitionDefinition {
	if n == nil {
		return nil
	}
	out := *n
	out.Name = DeepCloneIdentifierCI(n.Name)
	out.Options = DeepCloneRefOfSubPartitionDefinitionOptions(n.Options)
	return &out
}

// CloneRefOfSubPartitionDefinitionOptions creates a deep clone of the input.
func DeepCloneRefOfSubPartitionDefinitionOptions(n *SubPartitionDefinitionOptions) *SubPartitionDefinitionOptions {
	if n == nil {
		return nil
	}
	out := *n
	out.Comment = DeepCloneRefOfLiteral(n.Comment)
	out.Engine = DeepCloneRefOfPartitionEngine(n.Engine)
	out.DataDirectory = DeepCloneRefOfLiteral(n.DataDirectory)
	out.IndexDirectory = DeepCloneRefOfLiteral(n.IndexDirectory)
	out.MaxRows = DeepCloneRefOfInt(n.MaxRows)
	out.MinRows = DeepCloneRefOfInt(n.MinRows)
	return &out
}

// CloneSubPartitionDefinitions creates a deep clone of the input.
func DeepCloneSubPartitionDefinitions(n SubPartitionDefinitions) SubPartitionDefinitions {
	if n == nil {
		return nil
	}
	res := make(SubPartitionDefinitions, len(n))
	for i, x := range n {
		res[i] = DeepCloneRefOfSubPartitionDefinition(x)
	}
	return res
}

// CloneRefOfSubquery creates a deep clone of the input.
func DeepCloneRefOfSubquery(n *Subquery) *Subquery {
	if n == nil {
		return nil
	}
	out := *n
	out.Select = DeepCloneSelectStatement(n.Select)
	return &out
}

// CloneRefOfSubstrExpr creates a deep clone of the input.
func DeepCloneRefOfSubstrExpr(n *SubstrExpr) *SubstrExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Name = DeepCloneExpr(n.Name)
	out.From = DeepCloneExpr(n.From)
	out.To = DeepCloneExpr(n.To)
	return &out
}

// CloneRefOfSum creates a deep clone of the input.
func DeepCloneRefOfSum(n *Sum) *Sum {
	if n == nil {
		return nil
	}
	out := *n
	out.Arg = DeepCloneExpr(n.Arg)
	return &out
}

// CloneTableExprs creates a deep clone of the input.
func DeepCloneTableExprs(n TableExprs) TableExprs {
	if n == nil {
		return nil
	}
	res := make(TableExprs, len(n))
	for i, x := range n {
		res[i] = DeepCloneTableExpr(x)
	}
	return res
}

// CloneTableName creates a deep clone of the input.
func DeepCloneTableName(n TableName) TableName {
	return *CloneRefOfTableName(&n)
}

// CloneTableNames creates a deep clone of the input.
func DeepCloneTableNames(n TableNames) TableNames {
	if n == nil {
		return nil
	}
	res := make(TableNames, len(n))
	for i, x := range n {
		res[i] = DeepCloneTableName(x)
	}
	return res
}

// CloneTableOptions creates a deep clone of the input.
func DeepCloneTableOptions(n TableOptions) TableOptions {
	if n == nil {
		return nil
	}
	res := make(TableOptions, len(n))
	for i, x := range n {
		res[i] = DeepCloneRefOfTableOption(x)
	}
	return res
}

// CloneRefOfTableSpec creates a deep clone of the input.
func DeepCloneRefOfTableSpec(n *TableSpec) *TableSpec {
	if n == nil {
		return nil
	}
	out := *n
	out.Columns = DeepCloneSliceOfRefOfColumnDefinition(n.Columns)
	out.Indexes = DeepCloneSliceOfRefOfIndexDefinition(n.Indexes)
	out.Constraints = DeepCloneSliceOfRefOfConstraintDefinition(n.Constraints)
	out.Options = DeepCloneTableOptions(n.Options)
	out.PartitionOption = DeepCloneRefOfPartitionOption(n.PartitionOption)
	return &out
}

// CloneRefOfTablespaceOperation creates a deep clone of the input.
func DeepCloneRefOfTablespaceOperation(n *TablespaceOperation) *TablespaceOperation {
	if n == nil {
		return nil
	}
	out := *n
	return &out
}

// CloneRefOfTimestampDiffExpr creates a deep clone of the input.
func DeepCloneRefOfTimestampDiffExpr(n *TimestampDiffExpr) *TimestampDiffExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Expr1 = DeepCloneExpr(n.Expr1)
	out.Expr2 = DeepCloneExpr(n.Expr2)
	return &out
}

// CloneRefOfTrimFuncExpr creates a deep clone of the input.
func DeepCloneRefOfTrimFuncExpr(n *TrimFuncExpr) *TrimFuncExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.TrimArg = DeepCloneExpr(n.TrimArg)
	out.StringArg = DeepCloneExpr(n.StringArg)
	return &out
}

// CloneRefOfTruncateTable creates a deep clone of the input.
func DeepCloneRefOfTruncateTable(n *TruncateTable) *TruncateTable {
	if n == nil {
		return nil
	}
	out := *n
	out.Table = DeepCloneTableName(n.Table)
	return &out
}

// CloneRefOfUnaryExpr creates a deep clone of the input.
func DeepCloneRefOfUnaryExpr(n *UnaryExpr) *UnaryExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Expr = DeepCloneExpr(n.Expr)
	return &out
}

// CloneRefOfUnion creates a deep clone of the input.
func DeepCloneRefOfUnion(n *Union) *Union {
	if n == nil {
		return nil
	}
	out := *n
	out.Left = DeepCloneSelectStatement(n.Left)
	out.Right = DeepCloneSelectStatement(n.Right)
	out.OrderBy = DeepCloneOrderBy(n.OrderBy)
	out.With = DeepCloneRefOfWith(n.With)
	out.Limit = DeepCloneRefOfLimit(n.Limit)
	out.Into = DeepCloneRefOfSelectInto(n.Into)
	return &out
}

// CloneRefOfUnlockTables creates a deep clone of the input.
func DeepCloneRefOfUnlockTables(n *UnlockTables) *UnlockTables {
	if n == nil {
		return nil
	}
	out := *n
	return &out
}

// CloneRefOfUpdate creates a deep clone of the input.
func DeepCloneRefOfUpdate(n *Update) *Update {
	if n == nil {
		return nil
	}
	out := *n
	out.With = DeepCloneRefOfWith(n.With)
	out.Comments = DeepCloneRefOfParsedComments(n.Comments)
	out.TableExprs = DeepCloneTableExprs(n.TableExprs)
	out.Exprs = DeepCloneUpdateExprs(n.Exprs)
	out.Where = DeepCloneRefOfWhere(n.Where)
	out.OrderBy = DeepCloneOrderBy(n.OrderBy)
	out.Limit = DeepCloneRefOfLimit(n.Limit)
	return &out
}

// CloneRefOfUpdateExpr creates a deep clone of the input.
func DeepCloneRefOfUpdateExpr(n *UpdateExpr) *UpdateExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Name = DeepCloneRefOfColName(n.Name)
	out.Expr = DeepCloneExpr(n.Expr)
	return &out
}

// CloneUpdateExprs creates a deep clone of the input.
func DeepCloneUpdateExprs(n UpdateExprs) UpdateExprs {
	if n == nil {
		return nil
	}
	res := make(UpdateExprs, len(n))
	for i, x := range n {
		res[i] = DeepCloneRefOfUpdateExpr(x)
	}
	return res
}

// CloneRefOfUpdateXMLExpr creates a deep clone of the input.
func DeepCloneRefOfUpdateXMLExpr(n *UpdateXMLExpr) *UpdateXMLExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Target = DeepCloneExpr(n.Target)
	out.XPathExpr = DeepCloneExpr(n.XPathExpr)
	out.NewXML = DeepCloneExpr(n.NewXML)
	return &out
}

// CloneRefOfUse creates a deep clone of the input.
func DeepCloneRefOfUse(n *Use) *Use {
	if n == nil {
		return nil
	}
	out := *n
	out.DBName = DeepCloneIdentifierCS(n.DBName)
	return &out
}

// CloneRefOfVExplainStmt creates a deep clone of the input.
func DeepCloneRefOfVExplainStmt(n *VExplainStmt) *VExplainStmt {
	if n == nil {
		return nil
	}
	out := *n
	out.Statement = DeepCloneStatement(n.Statement)
	out.Comments = DeepCloneRefOfParsedComments(n.Comments)
	return &out
}

// CloneRefOfVStream creates a deep clone of the input.
func DeepCloneRefOfVStream(n *VStream) *VStream {
	if n == nil {
		return nil
	}
	out := *n
	out.Comments = DeepCloneRefOfParsedComments(n.Comments)
	out.SelectExpr = DeepCloneSelectExpr(n.SelectExpr)
	out.Table = DeepCloneTableName(n.Table)
	out.Where = DeepCloneRefOfWhere(n.Where)
	out.Limit = DeepCloneRefOfLimit(n.Limit)
	return &out
}

// CloneValTuple creates a deep clone of the input.
func DeepCloneValTuple(n ValTuple) ValTuple {
	if n == nil {
		return nil
	}
	res := make(ValTuple, len(n))
	for i, x := range n {
		res[i] = DeepCloneExpr(x)
	}
	return res
}

// CloneRefOfValidation creates a deep clone of the input.
func DeepCloneRefOfValidation(n *Validation) *Validation {
	if n == nil {
		return nil
	}
	out := *n
	return &out
}

// CloneValues creates a deep clone of the input.
func DeepCloneValues(n Values) Values {
	if n == nil {
		return nil
	}
	res := make(Values, len(n))
	for i, x := range n {
		res[i] = DeepCloneValTuple(x)
	}
	return res
}

// CloneRefOfValuesFuncExpr creates a deep clone of the input.
func DeepCloneRefOfValuesFuncExpr(n *ValuesFuncExpr) *ValuesFuncExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Name = DeepCloneRefOfColName(n.Name)
	return &out
}

// CloneRefOfVarPop creates a deep clone of the input.
func DeepCloneRefOfVarPop(n *VarPop) *VarPop {
	if n == nil {
		return nil
	}
	out := *n
	out.Arg = DeepCloneExpr(n.Arg)
	return &out
}

// CloneRefOfVarSamp creates a deep clone of the input.
func DeepCloneRefOfVarSamp(n *VarSamp) *VarSamp {
	if n == nil {
		return nil
	}
	out := *n
	out.Arg = DeepCloneExpr(n.Arg)
	return &out
}

// CloneRefOfVariable creates a deep clone of the input.
func DeepCloneRefOfVariable(n *Variable) *Variable {
	if n == nil {
		return nil
	}
	out := *n
	out.Name = DeepCloneIdentifierCI(n.Name)
	return &out
}

// CloneRefOfVariance creates a deep clone of the input.
func DeepCloneRefOfVariance(n *Variance) *Variance {
	if n == nil {
		return nil
	}
	out := *n
	out.Arg = DeepCloneExpr(n.Arg)
	return &out
}

// CloneVindexParam creates a deep clone of the input.
func DeepCloneVindexParam(n VindexParam) VindexParam {
	return *CloneRefOfVindexParam(&n)
}

// CloneRefOfVindexSpec creates a deep clone of the input.
func DeepCloneRefOfVindexSpec(n *VindexSpec) *VindexSpec {
	if n == nil {
		return nil
	}
	out := *n
	out.Name = DeepCloneIdentifierCI(n.Name)
	out.Type = DeepCloneIdentifierCI(n.Type)
	out.Params = DeepCloneSliceOfVindexParam(n.Params)
	return &out
}

// CloneRefOfWeightStringFuncExpr creates a deep clone of the input.
func DeepCloneRefOfWeightStringFuncExpr(n *WeightStringFuncExpr) *WeightStringFuncExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Expr = DeepCloneExpr(n.Expr)
	out.As = DeepCloneRefOfConvertType(n.As)
	return &out
}

// CloneRefOfWhen creates a deep clone of the input.
func DeepCloneRefOfWhen(n *When) *When {
	if n == nil {
		return nil
	}
	out := *n
	out.Cond = DeepCloneExpr(n.Cond)
	out.Val = DeepCloneExpr(n.Val)
	return &out
}

// CloneRefOfWhere creates a deep clone of the input.
func DeepCloneRefOfWhere(n *Where) *Where {
	if n == nil {
		return nil
	}
	out := *n
	out.Expr = DeepCloneExpr(n.Expr)
	return &out
}

// CloneRefOfWindowDefinition creates a deep clone of the input.
func DeepCloneRefOfWindowDefinition(n *WindowDefinition) *WindowDefinition {
	if n == nil {
		return nil
	}
	out := *n
	out.Name = DeepCloneIdentifierCI(n.Name)
	out.WindowSpec = DeepCloneRefOfWindowSpecification(n.WindowSpec)
	return &out
}

// CloneWindowDefinitions creates a deep clone of the input.
func DeepCloneWindowDefinitions(n WindowDefinitions) WindowDefinitions {
	if n == nil {
		return nil
	}
	res := make(WindowDefinitions, len(n))
	for i, x := range n {
		res[i] = DeepCloneRefOfWindowDefinition(x)
	}
	return res
}

// CloneRefOfWindowSpecification creates a deep clone of the input.
func DeepCloneRefOfWindowSpecification(n *WindowSpecification) *WindowSpecification {
	if n == nil {
		return nil
	}
	out := *n
	out.Name = DeepCloneIdentifierCI(n.Name)
	out.PartitionClause = DeepCloneExprs(n.PartitionClause)
	out.OrderClause = DeepCloneOrderBy(n.OrderClause)
	out.FrameClause = DeepCloneRefOfFrameClause(n.FrameClause)
	return &out
}

// CloneRefOfWith creates a deep clone of the input.
func DeepCloneRefOfWith(n *With) *With {
	if n == nil {
		return nil
	}
	out := *n
	out.ctes = DeepCloneSliceOfRefOfCommonTableExpr(n.ctes)
	return &out
}

// CloneRefOfXorExpr creates a deep clone of the input.
func DeepCloneRefOfXorExpr(n *XorExpr) *XorExpr {
	if n == nil {
		return nil
	}
	out := *n
	out.Left = DeepCloneExpr(n.Left)
	out.Right = DeepCloneExpr(n.Right)
	return &out
}

// CloneAggrFunc creates a deep clone of the input.
func DeepCloneAggrFunc(in AggrFunc) AggrFunc {
	if in == nil {
		return nil
	}
	switch in := in.(type) {
	case *AnyValue:
		return DeepCloneRefOfAnyValue(in)
	case *Avg:
		return DeepCloneRefOfAvg(in)
	case *BitAnd:
		return DeepCloneRefOfBitAnd(in)
	case *BitOr:
		return DeepCloneRefOfBitOr(in)
	case *BitXor:
		return DeepCloneRefOfBitXor(in)
	case *Count:
		return DeepCloneRefOfCount(in)
	case *CountStar:
		return DeepCloneRefOfCountStar(in)
	case *GroupConcatExpr:
		return DeepCloneRefOfGroupConcatExpr(in)
	case *Max:
		return DeepCloneRefOfMax(in)
	case *Min:
		return DeepCloneRefOfMin(in)
	case *Std:
		return DeepCloneRefOfStd(in)
	case *StdDev:
		return DeepCloneRefOfStdDev(in)
	case *StdPop:
		return DeepCloneRefOfStdPop(in)
	case *StdSamp:
		return DeepCloneRefOfStdSamp(in)
	case *Sum:
		return DeepCloneRefOfSum(in)
	case *VarPop:
		return DeepCloneRefOfVarPop(in)
	case *VarSamp:
		return DeepCloneRefOfVarSamp(in)
	case *Variance:
		return DeepCloneRefOfVariance(in)
	default:
		// this should never happen
		return nil
	}
}

// CloneAlterOption creates a deep clone of the input.
func DeepCloneAlterOption(in AlterOption) AlterOption {
	if in == nil {
		return nil
	}
	switch in := in.(type) {
	case *AddColumns:
		return DeepCloneRefOfAddColumns(in)
	case *AddConstraintDefinition:
		return DeepCloneRefOfAddConstraintDefinition(in)
	case *AddIndexDefinition:
		return DeepCloneRefOfAddIndexDefinition(in)
	case AlgorithmValue:
		return in
	case *AlterCharset:
		return DeepCloneRefOfAlterCharset(in)
	case *AlterCheck:
		return DeepCloneRefOfAlterCheck(in)
	case *AlterColumn:
		return DeepCloneRefOfAlterColumn(in)
	case *AlterIndex:
		return DeepCloneRefOfAlterIndex(in)
	case *ChangeColumn:
		return DeepCloneRefOfChangeColumn(in)
	case *DropColumn:
		return DeepCloneRefOfDropColumn(in)
	case *DropKey:
		return DeepCloneRefOfDropKey(in)
	case *Force:
		return DeepCloneRefOfForce(in)
	case *KeyState:
		return DeepCloneRefOfKeyState(in)
	case *LockOption:
		return DeepCloneRefOfLockOption(in)
	case *ModifyColumn:
		return DeepCloneRefOfModifyColumn(in)
	case *OrderByOption:
		return DeepCloneRefOfOrderByOption(in)
	case *RenameColumn:
		return DeepCloneRefOfRenameColumn(in)
	case *RenameIndex:
		return DeepCloneRefOfRenameIndex(in)
	case *RenameTableName:
		return DeepCloneRefOfRenameTableName(in)
	case TableOptions:
		return DeepCloneTableOptions(in)
	case *TablespaceOperation:
		return DeepCloneRefOfTablespaceOperation(in)
	case *Validation:
		return DeepCloneRefOfValidation(in)
	default:
		// this should never happen
		return nil
	}
}

// CloneCallable creates a deep clone of the input.
func DeepCloneCallable(in Callable) Callable {
	if in == nil {
		return nil
	}
	switch in := in.(type) {
	case *AnyValue:
		return DeepCloneRefOfAnyValue(in)
	case *ArgumentLessWindowExpr:
		return DeepCloneRefOfArgumentLessWindowExpr(in)
	case *Avg:
		return DeepCloneRefOfAvg(in)
	case *CharExpr:
		return DeepCloneRefOfCharExpr(in)
	case *ConvertExpr:
		return DeepCloneRefOfConvertExpr(in)
	case *ConvertUsingExpr:
		return DeepCloneRefOfConvertUsingExpr(in)
	case *Count:
		return DeepCloneRefOfCount(in)
	case *CountStar:
		return DeepCloneRefOfCountStar(in)
	case *CurTimeFuncExpr:
		return DeepCloneRefOfCurTimeFuncExpr(in)
	case *ExtractFuncExpr:
		return DeepCloneRefOfExtractFuncExpr(in)
	case *ExtractValueExpr:
		return DeepCloneRefOfExtractValueExpr(in)
	case *FirstOrLastValueExpr:
		return DeepCloneRefOfFirstOrLastValueExpr(in)
	case *FuncExpr:
		return DeepCloneRefOfFuncExpr(in)
	case *GTIDFuncExpr:
		return DeepCloneRefOfGTIDFuncExpr(in)
	case *GeoHashFromLatLongExpr:
		return DeepCloneRefOfGeoHashFromLatLongExpr(in)
	case *GeoHashFromPointExpr:
		return DeepCloneRefOfGeoHashFromPointExpr(in)
	case *GeoJSONFromGeomExpr:
		return DeepCloneRefOfGeoJSONFromGeomExpr(in)
	case *GeomCollPropertyFuncExpr:
		return DeepCloneRefOfGeomCollPropertyFuncExpr(in)
	case *GeomFormatExpr:
		return DeepCloneRefOfGeomFormatExpr(in)
	case *GeomFromGeoHashExpr:
		return DeepCloneRefOfGeomFromGeoHashExpr(in)
	case *GeomFromGeoJSONExpr:
		return DeepCloneRefOfGeomFromGeoJSONExpr(in)
	case *GeomFromTextExpr:
		return DeepCloneRefOfGeomFromTextExpr(in)
	case *GeomFromWKBExpr:
		return DeepCloneRefOfGeomFromWKBExpr(in)
	case *GeomPropertyFuncExpr:
		return DeepCloneRefOfGeomPropertyFuncExpr(in)
	case *GroupConcatExpr:
		return DeepCloneRefOfGroupConcatExpr(in)
	case *InsertExpr:
		return DeepCloneRefOfInsertExpr(in)
	case *IntervalDateExpr:
		return DeepCloneRefOfIntervalDateExpr(in)
	case *IntervalFuncExpr:
		return DeepCloneRefOfIntervalFuncExpr(in)
	case *JSONArrayExpr:
		return DeepCloneRefOfJSONArrayExpr(in)
	case *JSONAttributesExpr:
		return DeepCloneRefOfJSONAttributesExpr(in)
	case *JSONContainsExpr:
		return DeepCloneRefOfJSONContainsExpr(in)
	case *JSONContainsPathExpr:
		return DeepCloneRefOfJSONContainsPathExpr(in)
	case *JSONExtractExpr:
		return DeepCloneRefOfJSONExtractExpr(in)
	case *JSONKeysExpr:
		return DeepCloneRefOfJSONKeysExpr(in)
	case *JSONObjectExpr:
		return DeepCloneRefOfJSONObjectExpr(in)
	case *JSONOverlapsExpr:
		return DeepCloneRefOfJSONOverlapsExpr(in)
	case *JSONPrettyExpr:
		return DeepCloneRefOfJSONPrettyExpr(in)
	case *JSONQuoteExpr:
		return DeepCloneRefOfJSONQuoteExpr(in)
	case *JSONRemoveExpr:
		return DeepCloneRefOfJSONRemoveExpr(in)
	case *JSONSchemaValidFuncExpr:
		return DeepCloneRefOfJSONSchemaValidFuncExpr(in)
	case *JSONSchemaValidationReportFuncExpr:
		return DeepCloneRefOfJSONSchemaValidationReportFuncExpr(in)
	case *JSONSearchExpr:
		return DeepCloneRefOfJSONSearchExpr(in)
	case *JSONStorageFreeExpr:
		return DeepCloneRefOfJSONStorageFreeExpr(in)
	case *JSONStorageSizeExpr:
		return DeepCloneRefOfJSONStorageSizeExpr(in)
	case *JSONUnquoteExpr:
		return DeepCloneRefOfJSONUnquoteExpr(in)
	case *JSONValueExpr:
		return DeepCloneRefOfJSONValueExpr(in)
	case *JSONValueMergeExpr:
		return DeepCloneRefOfJSONValueMergeExpr(in)
	case *JSONValueModifierExpr:
		return DeepCloneRefOfJSONValueModifierExpr(in)
	case *LagLeadExpr:
		return DeepCloneRefOfLagLeadExpr(in)
	case *LineStringExpr:
		return DeepCloneRefOfLineStringExpr(in)
	case *LinestrPropertyFuncExpr:
		return DeepCloneRefOfLinestrPropertyFuncExpr(in)
	case *LocateExpr:
		return DeepCloneRefOfLocateExpr(in)
	case *MatchExpr:
		return DeepCloneRefOfMatchExpr(in)
	case *Max:
		return DeepCloneRefOfMax(in)
	case *MemberOfExpr:
		return DeepCloneRefOfMemberOfExpr(in)
	case *Min:
		return DeepCloneRefOfMin(in)
	case *MultiLinestringExpr:
		return DeepCloneRefOfMultiLinestringExpr(in)
	case *MultiPointExpr:
		return DeepCloneRefOfMultiPointExpr(in)
	case *MultiPolygonExpr:
		return DeepCloneRefOfMultiPolygonExpr(in)
	case *NTHValueExpr:
		return DeepCloneRefOfNTHValueExpr(in)
	case *NamedWindow:
		return DeepCloneRefOfNamedWindow(in)
	case *NtileExpr:
		return DeepCloneRefOfNtileExpr(in)
	case *PerformanceSchemaFuncExpr:
		return DeepCloneRefOfPerformanceSchemaFuncExpr(in)
	case *PointExpr:
		return DeepCloneRefOfPointExpr(in)
	case *PointPropertyFuncExpr:
		return DeepCloneRefOfPointPropertyFuncExpr(in)
	case *PolygonExpr:
		return DeepCloneRefOfPolygonExpr(in)
	case *PolygonPropertyFuncExpr:
		return DeepCloneRefOfPolygonPropertyFuncExpr(in)
	case *RegexpInstrExpr:
		return DeepCloneRefOfRegexpInstrExpr(in)
	case *RegexpLikeExpr:
		return DeepCloneRefOfRegexpLikeExpr(in)
	case *RegexpReplaceExpr:
		return DeepCloneRefOfRegexpReplaceExpr(in)
	case *RegexpSubstrExpr:
		return DeepCloneRefOfRegexpSubstrExpr(in)
	case *SubstrExpr:
		return DeepCloneRefOfSubstrExpr(in)
	case *Sum:
		return DeepCloneRefOfSum(in)
	case *TimestampDiffExpr:
		return DeepCloneRefOfTimestampDiffExpr(in)
	case *TrimFuncExpr:
		return DeepCloneRefOfTrimFuncExpr(in)
	case *UpdateXMLExpr:
		return DeepCloneRefOfUpdateXMLExpr(in)
	case *ValuesFuncExpr:
		return DeepCloneRefOfValuesFuncExpr(in)
	case *WeightStringFuncExpr:
		return DeepCloneRefOfWeightStringFuncExpr(in)
	default:
		// this should never happen
		return nil
	}
}

// CloneColTuple creates a deep clone of the input.
func DeepCloneColTuple(in ColTuple) ColTuple {
	if in == nil {
		return nil
	}
	switch in := in.(type) {
	case ListArg:
		return in
	case *Subquery:
		return DeepCloneRefOfSubquery(in)
	case ValTuple:
		return DeepCloneValTuple(in)
	default:
		// this should never happen
		return nil
	}
}

// CloneConstraintInfo creates a deep clone of the input.
func DeepCloneConstraintInfo(in ConstraintInfo) ConstraintInfo {
	if in == nil {
		return nil
	}
	switch in := in.(type) {
	case *CheckConstraintDefinition:
		return DeepCloneRefOfCheckConstraintDefinition(in)
	case *ForeignKeyDefinition:
		return DeepCloneRefOfForeignKeyDefinition(in)
	default:
		// this should never happen
		return nil
	}
}

// CloneDBDDLStatement creates a deep clone of the input.
func DeepCloneDBDDLStatement(in DBDDLStatement) DBDDLStatement {
	if in == nil {
		return nil
	}
	switch in := in.(type) {
	case *AlterDatabase:
		return DeepCloneRefOfAlterDatabase(in)
	case *CreateDatabase:
		return DeepCloneRefOfCreateDatabase(in)
	case *DropDatabase:
		return DeepCloneRefOfDropDatabase(in)
	default:
		// this should never happen
		return nil
	}
}

// CloneDDLStatement creates a deep clone of the input.
func DeepCloneDDLStatement(in DDLStatement) DDLStatement {
	if in == nil {
		return nil
	}
	switch in := in.(type) {
	case *AlterTable:
		return DeepCloneRefOfAlterTable(in)
	case *AlterView:
		return DeepCloneRefOfAlterView(in)
	case *CreateTable:
		return DeepCloneRefOfCreateTable(in)
	case *CreateView:
		return DeepCloneRefOfCreateView(in)
	case *DropTable:
		return DeepCloneRefOfDropTable(in)
	case *DropView:
		return DeepCloneRefOfDropView(in)
	case *RenameTable:
		return DeepCloneRefOfRenameTable(in)
	case *TruncateTable:
		return DeepCloneRefOfTruncateTable(in)
	default:
		// this should never happen
		return nil
	}
}

// CloneExplain creates a deep clone of the input.
func DeepCloneExplain(in Explain) Explain {
	if in == nil {
		return nil
	}
	switch in := in.(type) {
	case *ExplainStmt:
		return DeepCloneRefOfExplainStmt(in)
	case *ExplainTab:
		return DeepCloneRefOfExplainTab(in)
	default:
		// this should never happen
		return nil
	}
}

// CloneExpr creates a deep clone of the input.
func DeepCloneExpr(in Expr) Expr {
	if in == nil {
		return nil
	}
	switch in := in.(type) {
	case *AndExpr:
		return DeepCloneRefOfAndExpr(in)
	case *AnyValue:
		return DeepCloneRefOfAnyValue(in)
	case *Argument:
		return DeepCloneRefOfArgument(in)
	case *ArgumentLessWindowExpr:
		return DeepCloneRefOfArgumentLessWindowExpr(in)
	case *AssignmentExpr:
		return DeepCloneRefOfAssignmentExpr(in)
	case *Avg:
		return DeepCloneRefOfAvg(in)
	case *BetweenExpr:
		return DeepCloneRefOfBetweenExpr(in)
	case *BinaryExpr:
		return DeepCloneRefOfBinaryExpr(in)
	case *BitAnd:
		return DeepCloneRefOfBitAnd(in)
	case *BitOr:
		return DeepCloneRefOfBitOr(in)
	case *BitXor:
		return DeepCloneRefOfBitXor(in)
	case BoolVal:
		return in
	case *CaseExpr:
		return DeepCloneRefOfCaseExpr(in)
	case *CastExpr:
		return DeepCloneRefOfCastExpr(in)
	case *CharExpr:
		return DeepCloneRefOfCharExpr(in)
	case *ColName:
		return DeepCloneRefOfColName(in)
	case *CollateExpr:
		return DeepCloneRefOfCollateExpr(in)
	case *ComparisonExpr:
		return DeepCloneRefOfComparisonExpr(in)
	case *ConvertExpr:
		return DeepCloneRefOfConvertExpr(in)
	case *ConvertUsingExpr:
		return DeepCloneRefOfConvertUsingExpr(in)
	case *Count:
		return DeepCloneRefOfCount(in)
	case *CountStar:
		return DeepCloneRefOfCountStar(in)
	case *CurTimeFuncExpr:
		return DeepCloneRefOfCurTimeFuncExpr(in)
	case *Default:
		return DeepCloneRefOfDefault(in)
	case *ExistsExpr:
		return DeepCloneRefOfExistsExpr(in)
	case *ExtractFuncExpr:
		return DeepCloneRefOfExtractFuncExpr(in)
	case *ExtractValueExpr:
		return DeepCloneRefOfExtractValueExpr(in)
	case *FirstOrLastValueExpr:
		return DeepCloneRefOfFirstOrLastValueExpr(in)
	case *FuncExpr:
		return DeepCloneRefOfFuncExpr(in)
	case *GTIDFuncExpr:
		return DeepCloneRefOfGTIDFuncExpr(in)
	case *GeoHashFromLatLongExpr:
		return DeepCloneRefOfGeoHashFromLatLongExpr(in)
	case *GeoHashFromPointExpr:
		return DeepCloneRefOfGeoHashFromPointExpr(in)
	case *GeoJSONFromGeomExpr:
		return DeepCloneRefOfGeoJSONFromGeomExpr(in)
	case *GeomCollPropertyFuncExpr:
		return DeepCloneRefOfGeomCollPropertyFuncExpr(in)
	case *GeomFormatExpr:
		return DeepCloneRefOfGeomFormatExpr(in)
	case *GeomFromGeoHashExpr:
		return DeepCloneRefOfGeomFromGeoHashExpr(in)
	case *GeomFromGeoJSONExpr:
		return DeepCloneRefOfGeomFromGeoJSONExpr(in)
	case *GeomFromTextExpr:
		return DeepCloneRefOfGeomFromTextExpr(in)
	case *GeomFromWKBExpr:
		return DeepCloneRefOfGeomFromWKBExpr(in)
	case *GeomPropertyFuncExpr:
		return DeepCloneRefOfGeomPropertyFuncExpr(in)
	case *GroupConcatExpr:
		return DeepCloneRefOfGroupConcatExpr(in)
	case *InsertExpr:
		return DeepCloneRefOfInsertExpr(in)
	case *IntervalDateExpr:
		return DeepCloneRefOfIntervalDateExpr(in)
	case *IntervalFuncExpr:
		return DeepCloneRefOfIntervalFuncExpr(in)
	case *IntroducerExpr:
		return DeepCloneRefOfIntroducerExpr(in)
	case *IsExpr:
		return DeepCloneRefOfIsExpr(in)
	case *JSONArrayExpr:
		return DeepCloneRefOfJSONArrayExpr(in)
	case *JSONAttributesExpr:
		return DeepCloneRefOfJSONAttributesExpr(in)
	case *JSONContainsExpr:
		return DeepCloneRefOfJSONContainsExpr(in)
	case *JSONContainsPathExpr:
		return DeepCloneRefOfJSONContainsPathExpr(in)
	case *JSONExtractExpr:
		return DeepCloneRefOfJSONExtractExpr(in)
	case *JSONKeysExpr:
		return DeepCloneRefOfJSONKeysExpr(in)
	case *JSONObjectExpr:
		return DeepCloneRefOfJSONObjectExpr(in)
	case *JSONOverlapsExpr:
		return DeepCloneRefOfJSONOverlapsExpr(in)
	case *JSONPrettyExpr:
		return DeepCloneRefOfJSONPrettyExpr(in)
	case *JSONQuoteExpr:
		return DeepCloneRefOfJSONQuoteExpr(in)
	case *JSONRemoveExpr:
		return DeepCloneRefOfJSONRemoveExpr(in)
	case *JSONSchemaValidFuncExpr:
		return DeepCloneRefOfJSONSchemaValidFuncExpr(in)
	case *JSONSchemaValidationReportFuncExpr:
		return DeepCloneRefOfJSONSchemaValidationReportFuncExpr(in)
	case *JSONSearchExpr:
		return DeepCloneRefOfJSONSearchExpr(in)
	case *JSONStorageFreeExpr:
		return DeepCloneRefOfJSONStorageFreeExpr(in)
	case *JSONStorageSizeExpr:
		return DeepCloneRefOfJSONStorageSizeExpr(in)
	case *JSONUnquoteExpr:
		return DeepCloneRefOfJSONUnquoteExpr(in)
	case *JSONValueExpr:
		return DeepCloneRefOfJSONValueExpr(in)
	case *JSONValueMergeExpr:
		return DeepCloneRefOfJSONValueMergeExpr(in)
	case *JSONValueModifierExpr:
		return DeepCloneRefOfJSONValueModifierExpr(in)
	case *LagLeadExpr:
		return DeepCloneRefOfLagLeadExpr(in)
	case *LineStringExpr:
		return DeepCloneRefOfLineStringExpr(in)
	case *LinestrPropertyFuncExpr:
		return DeepCloneRefOfLinestrPropertyFuncExpr(in)
	case ListArg:
		return in
	case *Literal:
		return DeepCloneRefOfLiteral(in)
	case *LocateExpr:
		return DeepCloneRefOfLocateExpr(in)
	case *LockingFunc:
		return DeepCloneRefOfLockingFunc(in)
	case *MatchExpr:
		return DeepCloneRefOfMatchExpr(in)
	case *Max:
		return DeepCloneRefOfMax(in)
	case *MemberOfExpr:
		return DeepCloneRefOfMemberOfExpr(in)
	case *Min:
		return DeepCloneRefOfMin(in)
	case *MultiLinestringExpr:
		return DeepCloneRefOfMultiLinestringExpr(in)
	case *MultiPointExpr:
		return DeepCloneRefOfMultiPointExpr(in)
	case *MultiPolygonExpr:
		return DeepCloneRefOfMultiPolygonExpr(in)
	case *NTHValueExpr:
		return DeepCloneRefOfNTHValueExpr(in)
	case *NamedWindow:
		return DeepCloneRefOfNamedWindow(in)
	case *NotExpr:
		return DeepCloneRefOfNotExpr(in)
	case *NtileExpr:
		return DeepCloneRefOfNtileExpr(in)
	case *NullVal:
		return DeepCloneRefOfNullVal(in)
	case *Offset:
		return DeepCloneRefOfOffset(in)
	case *OrExpr:
		return DeepCloneRefOfOrExpr(in)
	case *PerformanceSchemaFuncExpr:
		return DeepCloneRefOfPerformanceSchemaFuncExpr(in)
	case *PointExpr:
		return DeepCloneRefOfPointExpr(in)
	case *PointPropertyFuncExpr:
		return DeepCloneRefOfPointPropertyFuncExpr(in)
	case *PolygonExpr:
		return DeepCloneRefOfPolygonExpr(in)
	case *PolygonPropertyFuncExpr:
		return DeepCloneRefOfPolygonPropertyFuncExpr(in)
	case *RegexpInstrExpr:
		return DeepCloneRefOfRegexpInstrExpr(in)
	case *RegexpLikeExpr:
		return DeepCloneRefOfRegexpLikeExpr(in)
	case *RegexpReplaceExpr:
		return DeepCloneRefOfRegexpReplaceExpr(in)
	case *RegexpSubstrExpr:
		return DeepCloneRefOfRegexpSubstrExpr(in)
	case *Std:
		return DeepCloneRefOfStd(in)
	case *StdDev:
		return DeepCloneRefOfStdDev(in)
	case *StdPop:
		return DeepCloneRefOfStdPop(in)
	case *StdSamp:
		return DeepCloneRefOfStdSamp(in)
	case *Subquery:
		return DeepCloneRefOfSubquery(in)
	case *SubstrExpr:
		return DeepCloneRefOfSubstrExpr(in)
	case *Sum:
		return DeepCloneRefOfSum(in)
	case *TimestampDiffExpr:
		return DeepCloneRefOfTimestampDiffExpr(in)
	case *TrimFuncExpr:
		return DeepCloneRefOfTrimFuncExpr(in)
	case *UnaryExpr:
		return DeepCloneRefOfUnaryExpr(in)
	case *UpdateXMLExpr:
		return DeepCloneRefOfUpdateXMLExpr(in)
	case ValTuple:
		return DeepCloneValTuple(in)
	case *ValuesFuncExpr:
		return DeepCloneRefOfValuesFuncExpr(in)
	case *VarPop:
		return DeepCloneRefOfVarPop(in)
	case *VarSamp:
		return DeepCloneRefOfVarSamp(in)
	case *Variable:
		return DeepCloneRefOfVariable(in)
	case *Variance:
		return DeepCloneRefOfVariance(in)
	case *WeightStringFuncExpr:
		return DeepCloneRefOfWeightStringFuncExpr(in)
	case *XorExpr:
		return DeepCloneRefOfXorExpr(in)
	default:
		// this should never happen
		return nil
	}
}

// CloneInsertRows creates a deep clone of the input.
func DeepCloneInsertRows(in InsertRows) InsertRows {
	if in == nil {
		return nil
	}
	switch in := in.(type) {
	case *Select:
		return DeepCloneRefOfSelect(in)
	case *Union:
		return DeepCloneRefOfUnion(in)
	case Values:
		return DeepCloneValues(in)
	default:
		// this should never happen
		return nil
	}
}

// CloneSelectExpr creates a deep clone of the input.
func DeepCloneSelectExpr(in SelectExpr) SelectExpr {
	if in == nil {
		return nil
	}
	switch in := in.(type) {
	case *AliasedExpr:
		return DeepCloneRefOfAliasedExpr(in)
	case *Nextval:
		return DeepCloneRefOfNextval(in)
	case *StarExpr:
		return DeepCloneRefOfStarExpr(in)
	default:
		// this should never happen
		return nil
	}
}

// CloneSelectStatement creates a deep clone of the input.
func DeepCloneSelectStatement(in SelectStatement) SelectStatement {
	if in == nil {
		return nil
	}
	switch in := in.(type) {
	case *Select:
		return DeepCloneRefOfSelect(in)
	case *Union:
		return DeepCloneRefOfUnion(in)
	default:
		// this should never happen
		return nil
	}
}

// CloneShowInternal creates a deep clone of the input.
func DeepCloneShowInternal(in ShowInternal) ShowInternal {
	if in == nil {
		return nil
	}
	switch in := in.(type) {
	case *ShowBasic:
		return DeepCloneRefOfShowBasic(in)
	case *ShowCreate:
		return DeepCloneRefOfShowCreate(in)
	case *ShowOther:
		return DeepCloneRefOfShowOther(in)
	default:
		// this should never happen
		return nil
	}
}

// CloneSimpleTableExpr creates a deep clone of the input.
func DeepCloneSimpleTableExpr(in SimpleTableExpr) SimpleTableExpr {
	if in == nil {
		return nil
	}
	switch in := in.(type) {
	case *DerivedTable:
		return DeepCloneRefOfDerivedTable(in)
	case TableName:
		return DeepCloneTableName(in)
	default:
		// this should never happen
		return nil
	}
}

// CloneStatement creates a deep clone of the input.
func DeepCloneStatement(in Statement) Statement {
	if in == nil {
		return nil
	}
	switch in := in.(type) {
	case *AlterDatabase:
		return DeepCloneRefOfAlterDatabase(in)
	case *AlterMigration:
		return DeepCloneRefOfAlterMigration(in)
	case *AlterTable:
		return DeepCloneRefOfAlterTable(in)
	case *AlterView:
		return DeepCloneRefOfAlterView(in)
	case *AlterVschema:
		return DeepCloneRefOfAlterVschema(in)
	case *Analyze:
		return DeepCloneRefOfAnalyze(in)
	case *Begin:
		return DeepCloneRefOfBegin(in)
	case *CallProc:
		return DeepCloneRefOfCallProc(in)
	case *CommentOnly:
		return DeepCloneRefOfCommentOnly(in)
	case *Commit:
		return DeepCloneRefOfCommit(in)
	case *CreateDatabase:
		return DeepCloneRefOfCreateDatabase(in)
	case *CreateTable:
		return DeepCloneRefOfCreateTable(in)
	case *CreateView:
		return DeepCloneRefOfCreateView(in)
	case *DeallocateStmt:
		return DeepCloneRefOfDeallocateStmt(in)
	case *Delete:
		return DeepCloneRefOfDelete(in)
	case *DropDatabase:
		return DeepCloneRefOfDropDatabase(in)
	case *DropTable:
		return DeepCloneRefOfDropTable(in)
	case *DropView:
		return DeepCloneRefOfDropView(in)
	case *ExecuteStmt:
		return DeepCloneRefOfExecuteStmt(in)
	case *ExplainStmt:
		return DeepCloneRefOfExplainStmt(in)
	case *ExplainTab:
		return DeepCloneRefOfExplainTab(in)
	case *Flush:
		return DeepCloneRefOfFlush(in)
	case *Insert:
		return DeepCloneRefOfInsert(in)
	case *Kill:
		return DeepCloneRefOfKill(in)
	case *Load:
		return DeepCloneRefOfLoad(in)
	case *LoadDataStmt:
		return DeepCloneRefOfLoadDataStmt(in)
	case *LockTables:
		return DeepCloneRefOfLockTables(in)
	case *OtherAdmin:
		return DeepCloneRefOfOtherAdmin(in)
	case *PrepareStmt:
		return DeepCloneRefOfPrepareStmt(in)
	case *PurgeBinaryLogs:
		return DeepCloneRefOfPurgeBinaryLogs(in)
	case *Release:
		return DeepCloneRefOfRelease(in)
	case *RenameTable:
		return DeepCloneRefOfRenameTable(in)
	case *RevertMigration:
		return DeepCloneRefOfRevertMigration(in)
	case *Rollback:
		return DeepCloneRefOfRollback(in)
	case *SRollback:
		return DeepCloneRefOfSRollback(in)
	case *Savepoint:
		return DeepCloneRefOfSavepoint(in)
	case *Select:
		return DeepCloneRefOfSelect(in)
	case *Set:
		return DeepCloneRefOfSet(in)
	case *Show:
		return DeepCloneRefOfShow(in)
	case *ShowMigrationLogs:
		return DeepCloneRefOfShowMigrationLogs(in)
	case *ShowThrottledApps:
		return DeepCloneRefOfShowThrottledApps(in)
	case *ShowThrottlerStatus:
		return DeepCloneRefOfShowThrottlerStatus(in)
	case *Stream:
		return DeepCloneRefOfStream(in)
	case *TruncateTable:
		return DeepCloneRefOfTruncateTable(in)
	case *Union:
		return DeepCloneRefOfUnion(in)
	case *UnlockTables:
		return DeepCloneRefOfUnlockTables(in)
	case *Update:
		return DeepCloneRefOfUpdate(in)
	case *Use:
		return DeepCloneRefOfUse(in)
	case *VExplainStmt:
		return DeepCloneRefOfVExplainStmt(in)
	case *VStream:
		return DeepCloneRefOfVStream(in)
	default:
		// this should never happen
		return nil
	}
}

// CloneTableExpr creates a deep clone of the input.
func DeepCloneTableExpr(in TableExpr) TableExpr {
	if in == nil {
		return nil
	}
	switch in := in.(type) {
	case *AliasedTableExpr:
		return DeepCloneRefOfAliasedTableExpr(in)
	case *JSONTableExpr:
		return DeepCloneRefOfJSONTableExpr(in)
	case *JoinTableExpr:
		return DeepCloneRefOfJoinTableExpr(in)
	case *ParenTableExpr:
		return DeepCloneRefOfParenTableExpr(in)
	default:
		// this should never happen
		return nil
	}
}

// CloneSliceOfRefOfColumnDefinition creates a deep clone of the input.
func DeepCloneSliceOfRefOfColumnDefinition(n []*ColumnDefinition) []*ColumnDefinition {
	if n == nil {
		return nil
	}
	res := make([]*ColumnDefinition, len(n))
	for i, x := range n {
		res[i] = DeepCloneRefOfColumnDefinition(x)
	}
	return res
}

// CloneRefOfBool creates a deep clone of the input.
func DeepCloneRefOfBool(n *bool) *bool {
	if n == nil {
		return nil
	}
	out := *n
	return &out
}

// CloneSliceOfDatabaseOption creates a deep clone of the input.
func DeepCloneSliceOfDatabaseOption(n []DatabaseOption) []DatabaseOption {
	if n == nil {
		return nil
	}
	res := make([]DatabaseOption, len(n))
	for i, x := range n {
		res[i] = DeepCloneDatabaseOption(x)
	}
	return res
}

// CloneSliceOfAlterOption creates a deep clone of the input.
func DeepCloneSliceOfAlterOption(n []AlterOption) []AlterOption {
	if n == nil {
		return nil
	}
	res := make([]AlterOption, len(n))
	for i, x := range n {
		res[i] = DeepCloneAlterOption(x)
	}
	return res
}

// CloneSliceOfIdentifierCI creates a deep clone of the input.
func DeepCloneSliceOfIdentifierCI(n []IdentifierCI) []IdentifierCI {
	if n == nil {
		return nil
	}
	res := make([]IdentifierCI, len(n))
	for i, x := range n {
		res[i] = DeepCloneIdentifierCI(x)
	}
	return res
}

// CloneSliceOfTxAccessMode creates a deep clone of the input.
func DeepCloneSliceOfTxAccessMode(n []TxAccessMode) []TxAccessMode {
	if n == nil {
		return nil
	}
	res := make([]TxAccessMode, len(n))
	copy(res, n)
	return res
}

// CloneSliceOfRefOfWhen creates a deep clone of the input.
func DeepCloneSliceOfRefOfWhen(n []*When) []*When {
	if n == nil {
		return nil
	}
	res := make([]*When, len(n))
	for i, x := range n {
		res[i] = DeepCloneRefOfWhen(x)
	}
	return res
}

// CloneRefOfColumnTypeOptions creates a deep clone of the input.
func DeepCloneRefOfColumnTypeOptions(n *ColumnTypeOptions) *ColumnTypeOptions {
	if n == nil {
		return nil
	}
	out := *n
	out.Null = DeepCloneRefOfBool(n.Null)
	out.Default = DeepCloneExpr(n.Default)
	out.OnUpdate = DeepCloneExpr(n.OnUpdate)
	out.As = DeepCloneExpr(n.As)
	out.Comment = DeepCloneRefOfLiteral(n.Comment)
	out.Reference = DeepCloneRefOfReferenceDefinition(n.Reference)
	out.Invisible = DeepCloneRefOfBool(n.Invisible)
	out.EngineAttribute = DeepCloneRefOfLiteral(n.EngineAttribute)
	out.SecondaryEngineAttribute = DeepCloneRefOfLiteral(n.SecondaryEngineAttribute)
	out.SRID = DeepCloneRefOfLiteral(n.SRID)
	return &out
}

// CloneColumnCharset creates a deep clone of the input.
func DeepCloneColumnCharset(n ColumnCharset) ColumnCharset {
	return *CloneRefOfColumnCharset(&n)
}

// CloneSliceOfString creates a deep clone of the input.
func DeepCloneSliceOfString(n []string) []string {
	if n == nil {
		return nil
	}
	res := make([]string, len(n))
	copy(res, n)
	return res
}

// CloneSliceOfRefOfVariable creates a deep clone of the input.
func DeepCloneSliceOfRefOfVariable(n []*Variable) []*Variable {
	if n == nil {
		return nil
	}
	res := make([]*Variable, len(n))
	for i, x := range n {
		res[i] = DeepCloneRefOfVariable(x)
	}
	return res
}

// CloneRefOfIdentifierCI creates a deep clone of the input.
func DeepCloneRefOfIdentifierCI(n *IdentifierCI) *IdentifierCI {
	if n == nil {
		return nil
	}
	out := *n
	return &out
}

// CloneRefOfIdentifierCS creates a deep clone of the input.
func DeepCloneRefOfIdentifierCS(n *IdentifierCS) *IdentifierCS {
	if n == nil {
		return nil
	}
	out := *n
	return &out
}

// CloneSliceOfRefOfIndexColumn creates a deep clone of the input.
func DeepCloneSliceOfRefOfIndexColumn(n []*IndexColumn) []*IndexColumn {
	if n == nil {
		return nil
	}
	res := make([]*IndexColumn, len(n))
	for i, x := range n {
		res[i] = DeepCloneRefOfIndexColumn(x)
	}
	return res
}

// CloneSliceOfRefOfIndexOption creates a deep clone of the input.
func DeepCloneSliceOfRefOfIndexOption(n []*IndexOption) []*IndexOption {
	if n == nil {
		return nil
	}
	res := make([]*IndexOption, len(n))
	for i, x := range n {
		res[i] = DeepCloneRefOfIndexOption(x)
	}
	return res
}

// CloneSliceOfExpr creates a deep clone of the input.
func DeepCloneSliceOfExpr(n []Expr) []Expr {
	if n == nil {
		return nil
	}
	res := make([]Expr, len(n))
	for i, x := range n {
		res[i] = DeepCloneExpr(x)
	}
	return res
}

// CloneSliceOfRefOfJSONObjectParam creates a deep clone of the input.
func DeepCloneSliceOfRefOfJSONObjectParam(n []*JSONObjectParam) []*JSONObjectParam {
	if n == nil {
		return nil
	}
	res := make([]*JSONObjectParam, len(n))
	for i, x := range n {
		res[i] = DeepCloneRefOfJSONObjectParam(x)
	}
	return res
}

// CloneSliceOfRefOfJtColumnDefinition creates a deep clone of the input.
func DeepCloneSliceOfRefOfJtColumnDefinition(n []*JtColumnDefinition) []*JtColumnDefinition {
	if n == nil {
		return nil
	}
	res := make([]*JtColumnDefinition, len(n))
	for i, x := range n {
		res[i] = DeepCloneRefOfJtColumnDefinition(x)
	}
	return res
}

// CloneRefOfJtOrdinalColDef creates a deep clone of the input.
func DeepCloneRefOfJtOrdinalColDef(n *JtOrdinalColDef) *JtOrdinalColDef {
	if n == nil {
		return nil
	}
	out := *n
	out.Name = DeepCloneIdentifierCI(n.Name)
	return &out
}

// CloneRefOfJtPathColDef creates a deep clone of the input.
func DeepCloneRefOfJtPathColDef(n *JtPathColDef) *JtPathColDef {
	if n == nil {
		return nil
	}
	out := *n
	out.Name = DeepCloneIdentifierCI(n.Name)
	out.Type = DeepCloneRefOfColumnType(n.Type)
	out.Path = DeepCloneExpr(n.Path)
	out.EmptyOnResponse = DeepCloneRefOfJtOnResponse(n.EmptyOnResponse)
	out.ErrorOnResponse = DeepCloneRefOfJtOnResponse(n.ErrorOnResponse)
	return &out
}

// CloneRefOfJtNestedPathColDef creates a deep clone of the input.
func DeepCloneRefOfJtNestedPathColDef(n *JtNestedPathColDef) *JtNestedPathColDef {
	if n == nil {
		return nil
	}
	out := *n
	out.Path = DeepCloneExpr(n.Path)
	out.Columns = DeepCloneSliceOfRefOfJtColumnDefinition(n.Columns)
	return &out
}

// CloneTableAndLockTypes creates a deep clone of the input.
func DeepCloneTableAndLockTypes(n TableAndLockTypes) TableAndLockTypes {
	if n == nil {
		return nil
	}
	res := make(TableAndLockTypes, len(n))
	for i, x := range n {
		res[i] = DeepCloneRefOfTableAndLockType(x)
	}
	return res
}

// CloneSliceOfRefOfColName creates a deep clone of the input.
func DeepCloneSliceOfRefOfColName(n []*ColName) []*ColName {
	if n == nil {
		return nil
	}
	res := make([]*ColName, len(n))
	for i, x := range n {
		res[i] = DeepCloneRefOfColName(x)
	}
	return res
}

// CloneComments creates a deep clone of the input.
func DeepCloneComments(n Comments) Comments {
	if n == nil {
		return nil
	}
	res := make(Comments, len(n))
	for i, x := range n {
		res[i] = x
	}
	return res
}

// CloneRefOfInt creates a deep clone of the input.
func DeepCloneRefOfInt(n *int) *int {
	if n == nil {
		return nil
	}
	out := *n
	return &out
}

// CloneSliceOfRefOfPartitionDefinition creates a deep clone of the input.
func DeepCloneSliceOfRefOfPartitionDefinition(n []*PartitionDefinition) []*PartitionDefinition {
	if n == nil {
		return nil
	}
	res := make([]*PartitionDefinition, len(n))
	for i, x := range n {
		res[i] = DeepCloneRefOfPartitionDefinition(x)
	}
	return res
}

// CloneSliceOfRefOfRenameTablePair creates a deep clone of the input.
func DeepCloneSliceOfRefOfRenameTablePair(n []*RenameTablePair) []*RenameTablePair {
	if n == nil {
		return nil
	}
	res := make([]*RenameTablePair, len(n))
	for i, x := range n {
		res[i] = DeepCloneRefOfRenameTablePair(x)
	}
	return res
}

// CloneRefOfRootNode creates a deep clone of the input.
func DeepCloneRefOfRootNode(n *RootNode) *RootNode {
	if n == nil {
		return nil
	}
	out := *n
	out.SQLNode = DeepCloneSQLNode(n.SQLNode)
	return &out
}

// CloneSliceOfTableExpr creates a deep clone of the input.
func DeepCloneSliceOfTableExpr(n []TableExpr) []TableExpr {
	if n == nil {
		return nil
	}
	res := make([]TableExpr, len(n))
	for i, x := range n {
		res[i] = DeepCloneTableExpr(x)
	}
	return res
}

// CloneRefOfTableName creates a deep clone of the input.
func DeepCloneRefOfTableName(n *TableName) *TableName {
	if n == nil {
		return nil
	}
	out := *n
	out.Name = DeepCloneIdentifierCS(n.Name)
	out.Qualifier = DeepCloneIdentifierCS(n.Qualifier)
	return &out
}

// CloneRefOfTableOption creates a deep clone of the input.
func DeepCloneRefOfTableOption(n *TableOption) *TableOption {
	if n == nil {
		return nil
	}
	out := *n
	out.Value = DeepCloneRefOfLiteral(n.Value)
	out.Tables = DeepCloneTableNames(n.Tables)
	return &out
}

// CloneSliceOfRefOfIndexDefinition creates a deep clone of the input.
func DeepCloneSliceOfRefOfIndexDefinition(n []*IndexDefinition) []*IndexDefinition {
	if n == nil {
		return nil
	}
	res := make([]*IndexDefinition, len(n))
	for i, x := range n {
		res[i] = DeepCloneRefOfIndexDefinition(x)
	}
	return res
}

// CloneSliceOfRefOfConstraintDefinition creates a deep clone of the input.
func DeepCloneSliceOfRefOfConstraintDefinition(n []*ConstraintDefinition) []*ConstraintDefinition {
	if n == nil {
		return nil
	}
	res := make([]*ConstraintDefinition, len(n))
	for i, x := range n {
		res[i] = DeepCloneRefOfConstraintDefinition(x)
	}
	return res
}

// CloneRefOfVindexParam creates a deep clone of the input.
func DeepCloneRefOfVindexParam(n *VindexParam) *VindexParam {
	if n == nil {
		return nil
	}
	out := *n
	out.Key = DeepCloneIdentifierCI(n.Key)
	return &out
}

// CloneSliceOfVindexParam creates a deep clone of the input.
func DeepCloneSliceOfVindexParam(n []VindexParam) []VindexParam {
	if n == nil {
		return nil
	}
	res := make([]VindexParam, len(n))
	for i, x := range n {
		res[i] = DeepCloneVindexParam(x)
	}
	return res
}

// CloneSliceOfRefOfCommonTableExpr creates a deep clone of the input.
func DeepCloneSliceOfRefOfCommonTableExpr(n []*CommonTableExpr) []*CommonTableExpr {
	if n == nil {
		return nil
	}
	res := make([]*CommonTableExpr, len(n))
	for i, x := range n {
		res[i] = DeepCloneRefOfCommonTableExpr(x)
	}
	return res
}

// CloneDatabaseOption creates a deep clone of the input.
func DeepCloneDatabaseOption(n DatabaseOption) DatabaseOption {
	return *CloneRefOfDatabaseOption(&n)
}

// CloneRefOfColumnCharset creates a deep clone of the input.
func DeepCloneRefOfColumnCharset(n *ColumnCharset) *ColumnCharset {
	if n == nil {
		return nil
	}
	out := *n
	return &out
}

// CloneRefOfIndexColumn creates a deep clone of the input.
func DeepCloneRefOfIndexColumn(n *IndexColumn) *IndexColumn {
	if n == nil {
		return nil
	}
	out := *n
	out.Column = DeepCloneIdentifierCI(n.Column)
	out.Length = DeepCloneRefOfLiteral(n.Length)
	out.Expression = DeepCloneExpr(n.Expression)
	return &out
}

// CloneRefOfIndexOption creates a deep clone of the input.
func DeepCloneRefOfIndexOption(n *IndexOption) *IndexOption {
	if n == nil {
		return nil
	}
	out := *n
	out.Value = DeepCloneRefOfLiteral(n.Value)
	return &out
}

// CloneRefOfTableAndLockType creates a deep clone of the input.
func DeepCloneRefOfTableAndLockType(n *TableAndLockType) *TableAndLockType {
	if n == nil {
		return nil
	}
	out := *n
	out.Table = DeepCloneTableExpr(n.Table)
	return &out
}

// CloneRefOfRenameTablePair creates a deep clone of the input.
func DeepCloneRefOfRenameTablePair(n *RenameTablePair) *RenameTablePair {
	if n == nil {
		return nil
	}
	out := *n
	out.FromTable = DeepCloneTableName(n.FromTable)
	out.ToTable = DeepCloneTableName(n.ToTable)
	return &out
}

// CloneRefOfDatabaseOption creates a deep clone of the input.
func DeepCloneRefOfDatabaseOption(n *DatabaseOption) *DatabaseOption {
	if n == nil {
		return nil
	}
	out := *n
	return &out
}
