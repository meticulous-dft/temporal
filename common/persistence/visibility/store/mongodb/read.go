package mongodb

import (
	"context"
	"errors"

	"github.com/temporalio/sqlparser"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/visibilityservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute/sadefs"
)

type (
	listExecutionsRequest struct {
		namespaceID   namespace.ID
		namespaceName namespace.Name
		query         string
		pageSize      int
		nextPageToken []byte
		chasmMapper   *chasm.VisibilitySearchAttributesMapper
		archetypeID   chasm.ArchetypeID
	}

	pageToken struct {
		Values []any `bson:"values"`
	}

	sortField struct {
		name string
		desc bool
	}
)

func (s *VisibilityStore) ListWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequestV2,
) (*store.InternalListExecutionsResponse, error) {
	return s.listExecutions(ctx, listExecutionsRequest{
		namespaceID:   request.NamespaceID,
		namespaceName: request.Namespace,
		query:         request.Query,
		pageSize:      request.PageSize,
		nextPageToken: request.NextPageToken,
	})
}

func (s *VisibilityStore) ListChasmExecutions(
	ctx context.Context,
	request *visibilityservice.ListChasmExecutionsRequest,
) (*store.InternalListExecutionsResponse, error) {
	mapper, err := s.chasmMapper(request.ArchetypeId)
	if err != nil {
		return nil, err
	}
	return s.listExecutions(ctx, listExecutionsRequest{
		namespaceID:   namespace.ID(request.NamespaceId),
		namespaceName: namespace.Name(request.Namespace),
		query:         request.Query,
		pageSize:      int(request.PageSize),
		nextPageToken: request.NextPageToken,
		chasmMapper:   mapper,
		archetypeID:   request.ArchetypeId,
	})
}

func (s *VisibilityStore) listExecutions(
	ctx context.Context,
	request listExecutionsRequest,
) (*store.InternalListExecutionsResponse, error) {
	queryParams, err := s.buildQuery(
		request.namespaceID,
		request.namespaceName,
		request.query,
		request.chasmMapper,
		request.archetypeID,
	)
	if err != nil {
		return nil, err
	}
	sortFields, sortDocument, err := buildSort(queryParams)
	if err != nil {
		return nil, err
	}
	filter := queryParams.QueryExpr
	if len(request.nextPageToken) > 0 {
		paginationFilter, err := buildPaginationFilter(request.nextPageToken, sortFields)
		if err != nil {
			return nil, err
		}
		filter = andFilters(filter, paginationFilter)
	}
	cursor, err := s.collection.Find(
		ctx,
		filter,
		options.Find().SetSort(sortDocument).SetLimit(int64(request.pageSize)),
	)
	if err != nil {
		return nil, convertMongoError("list visibility documents", err)
	}
	defer func() { _ = cursor.Close(ctx) }()
	var documents []bson.M
	if err := cursor.All(ctx, &documents); err != nil {
		return nil, convertMongoError("decode visibility documents", err)
	}
	response := &store.InternalListExecutionsResponse{
		Executions: make([]*store.InternalExecutionInfo, len(documents)),
	}
	for index, document := range documents {
		response.Executions[index], err = s.parseDocument(document, request.chasmMapper)
		if err != nil {
			return nil, err
		}
	}
	if len(documents) == request.pageSize && len(documents) > 0 {
		response.NextPageToken, err = encodePageToken(documents[len(documents)-1], sortFields)
		if err != nil {
			return nil, err
		}
	}
	return response, nil
}

func (s *VisibilityStore) CountWorkflowExecutions(
	ctx context.Context,
	request *manager.CountWorkflowExecutionsRequest,
) (*store.InternalCountExecutionsResponse, error) {
	return s.countExecutions(
		ctx,
		request.NamespaceID,
		request.Namespace,
		request.Query,
		nil,
		chasm.UnspecifiedArchetypeID,
	)
}

func (s *VisibilityStore) CountChasmExecutions(
	ctx context.Context,
	request *visibilityservice.CountChasmExecutionsRequest,
) (*store.InternalCountExecutionsResponse, error) {
	mapper, err := s.chasmMapper(request.ArchetypeId)
	if err != nil {
		return nil, err
	}
	return s.countExecutions(
		ctx,
		namespace.ID(request.NamespaceId),
		namespace.Name(request.Namespace),
		request.Query,
		mapper,
		request.ArchetypeId,
	)
}

func (s *VisibilityStore) countExecutions(
	ctx context.Context,
	namespaceID namespace.ID,
	namespaceName namespace.Name,
	queryString string,
	chasmMapper *chasm.VisibilitySearchAttributesMapper,
	archetypeID chasm.ArchetypeID,
) (*store.InternalCountExecutionsResponse, error) {
	queryParams, err := s.buildQuery(namespaceID, namespaceName, queryString, chasmMapper, archetypeID)
	if err != nil {
		return nil, err
	}
	if len(queryParams.GroupBy) == 0 {
		count, err := s.collection.CountDocuments(ctx, queryParams.QueryExpr)
		if err != nil {
			return nil, convertMongoError("count visibility documents", err)
		}
		return &store.InternalCountExecutionsResponse{Count: count}, nil
	}
	return s.countGroupedExecutions(ctx, queryParams.QueryExpr, queryParams.GroupBy[0], chasmMapper)
}

func (s *VisibilityStore) countGroupedExecutions(
	ctx context.Context,
	filter bson.D,
	groupBy *query.SAColumn,
	chasmMapper *chasm.VisibilitySearchAttributesMapper,
) (*store.InternalCountExecutionsResponse, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: filter}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$" + groupBy.FieldName},
			{Key: "count", Value: bson.D{{Key: "$sum", Value: 1}}},
		}}},
		{{Key: "$sort", Value: bson.D{{Key: "_id", Value: 1}}}},
	}
	cursor, err := s.collection.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, convertMongoError("group visibility documents", err)
	}
	defer func() { _ = cursor.Close(ctx) }()
	var rows []struct {
		Value any   `bson:"_id"`
		Count int64 `bson:"count"`
	}
	if err := cursor.All(ctx, &rows); err != nil {
		return nil, convertMongoError("decode visibility groups", err)
	}
	typeMap, err := s.searchAttributesProvider.GetSearchAttributes(s.indexName, false)
	if err != nil {
		return nil, serviceerror.NewUnavailablef("unable to read search attribute types: %v", err)
	}
	combinedTypeMap := store.CombineTypeMaps(typeMap, chasmMapper)
	valueType, err := combinedTypeMap.GetType(groupBy.FieldName)
	if err != nil {
		return nil, err
	}
	response := &store.InternalCountExecutionsResponse{
		Groups: make([]store.InternalAggregationGroup, 0, len(rows)),
	}
	for _, row := range rows {
		payload, err := sadefs.EncodeValue(row.Value, valueType)
		if err != nil {
			return nil, err
		}
		response.Groups = append(response.Groups, store.InternalAggregationGroup{
			GroupValues: []*commonpb.Payload{payload},
			Count:       row.Count,
		})
		response.Count += row.Count
	}
	return response, nil
}

func (s *VisibilityStore) GetWorkflowExecution(
	ctx context.Context,
	request *manager.GetWorkflowExecutionRequest,
) (*store.InternalGetWorkflowExecutionResponse, error) {
	filter := bson.D{
		{Key: documentIDField, Value: documentID(request.NamespaceID.String(), request.RunID)},
		{Key: deletedField, Value: false},
	}
	var document bson.M
	if err := s.collection.FindOne(ctx, filter).Decode(&document); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, serviceerror.NewNotFound("workflow execution visibility record not found")
		}
		return nil, convertMongoError("get visibility document", err)
	}
	info, err := s.parseDocument(document, nil)
	if err != nil {
		return nil, err
	}
	return &store.InternalGetWorkflowExecutionResponse{Execution: info}, nil
}

func (s *VisibilityStore) buildQuery(
	namespaceID namespace.ID,
	namespaceName namespace.Name,
	queryString string,
	chasmMapper *chasm.VisibilitySearchAttributesMapper,
	archetypeID chasm.ArchetypeID,
) (result *query.QueryParams[bson.D], err error) {
	defer func() {
		var converterError *query.ConverterError
		if errors.As(err, &converterError) {
			err = converterError.ToInvalidArgument()
		}
	}()
	typeMap, err := s.searchAttributesProvider.GetSearchAttributes(s.indexName, false)
	if err != nil {
		return nil, serviceerror.NewUnavailablef("unable to read search attribute types: %v", err)
	}
	mapper, err := s.searchAttributesMapperProvider.GetMapper(namespaceName)
	if err != nil {
		return nil, err
	}
	converter := query.NewQueryConverter(&queryConverter{}, namespaceName, typeMap, mapper).
		WithChasmMapper(chasmMapper).
		WithArchetypeID(archetypeID)
	result, err = converter.Convert(queryString)
	if err != nil {
		return nil, err
	}
	result.QueryExpr = andFilters(
		bson.D{{Key: sadefs.NamespaceID, Value: namespaceID.String()}},
		bson.D{{Key: deletedField, Value: false}},
		result.QueryExpr,
	)
	return result, nil
}

func (s *VisibilityStore) chasmMapper(archetypeID chasm.ArchetypeID) (*chasm.VisibilitySearchAttributesMapper, error) {
	component, ok := s.chasmRegistry.ComponentByID(archetypeID)
	if !ok {
		return nil, serviceerror.NewInvalidArgumentf("unknown archetype ID: %d", archetypeID)
	}
	return component.SearchAttributesMapper(), nil
}

func andFilters(filters ...bson.D) bson.D {
	valid := make([]bson.D, 0, len(filters))
	for _, filter := range filters {
		if len(filter) > 0 {
			valid = append(valid, filter)
		}
	}
	if len(valid) == 1 {
		return valid[0]
	}
	values := make(bson.A, len(valid))
	for i, filter := range valid {
		values[i] = filter
	}
	return bson.D{{Key: "$and", Value: values}}
}

func buildSort(queryParams *query.QueryParams[bson.D]) ([]sortField, bson.D, error) {
	fields := make([]sortField, 0, len(queryParams.OrderBy)+3)
	if len(queryParams.OrderBy) == 0 {
		fields = append(fields,
			sortField{name: closeTimeSortField, desc: true},
			sortField{name: sadefs.StartTime, desc: true},
		)
	} else {
		for _, expression := range queryParams.OrderBy {
			column, ok := expression.Expr.(*query.SAColumn)
			if !ok {
				return nil, nil, query.NewConverterError(
					"%s: unexpected field in 'ORDER BY' clause: %s",
					query.NotSupportedErrMessage,
					sqlparser.String(expression),
				)
			}
			fieldName := column.FieldName
			if fieldName == sadefs.CloseTime {
				fieldName = closeTimeSortField
			}
			fields = append(fields, sortField{
				name: fieldName,
				desc: expression.Direction == sqlparser.DescScr,
			})
		}
	}
	fields = append(fields, sortField{name: sadefs.RunID, desc: true})
	sortDocument := make(bson.D, len(fields))
	for index, field := range fields {
		direction := 1
		if field.desc {
			direction = -1
		}
		sortDocument[index] = bson.E{Key: field.name, Value: direction}
	}
	return fields, sortDocument, nil
}

func encodePageToken(document bson.M, fields []sortField) ([]byte, error) {
	values := make([]any, len(fields))
	for index, field := range fields {
		values[index] = document[field.name]
	}
	data, err := bson.Marshal(pageToken{Values: values})
	if err != nil {
		return nil, serviceerror.NewInternalf("unable to serialize visibility page token: %v", err)
	}
	return data, nil
}

func buildPaginationFilter(data []byte, fields []sortField) (bson.D, error) {
	var token pageToken
	if err := bson.Unmarshal(data, &token); err != nil {
		return nil, serviceerror.NewInvalidArgumentf("unable to deserialize visibility page token: %v", err)
	}
	if len(token.Values) != len(fields) {
		return nil, serviceerror.NewInvalidArgumentf(
			"visibility page token contains %d sort values, expected %d",
			len(token.Values),
			len(fields),
		)
	}
	alternatives := make(bson.A, 0, len(fields))
	for index, field := range fields {
		parts := make(bson.A, 0, index+1)
		for prefix := 0; prefix < index; prefix++ {
			parts = append(parts, bson.D{{Key: fields[prefix].name, Value: token.Values[prefix]}})
		}
		operator := "$gt"
		if field.desc {
			operator = "$lt"
		}
		parts = append(parts, bson.D{{Key: field.name, Value: bson.D{{Key: operator, Value: token.Values[index]}}}})
		alternatives = append(alternatives, bson.D{{Key: "$and", Value: parts}})
	}
	return bson.D{{Key: "$or", Value: alternatives}}, nil
}
