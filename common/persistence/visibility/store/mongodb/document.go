package mongodb

import (
	"errors"
	"fmt"
	"math"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/searchattribute/sadefs"
)

const (
	documentIDField    = "_id"
	versionField       = "_version"
	deletedField       = "_deleted"
	closeTimeSortField = "_close_time_sort"
)

var maxDatetime = time.Unix(0, math.MaxInt64).UTC()

func documentID(namespaceID string, runID string) string {
	return namespaceID + "\x00" + runID
}

func (s *VisibilityStore) buildDocument(
	request *store.InternalVisibilityRequestBase,
) (bson.M, error) {
	document := bson.M{
		documentIDField:        documentID(request.NamespaceID, request.RunID),
		versionField:           request.TaskID,
		deletedField:           false,
		closeTimeSortField:     maxDatetime.UnixNano(),
		sadefs.NamespaceID:     request.NamespaceID,
		sadefs.WorkflowID:      request.WorkflowID,
		sadefs.RunID:           request.RunID,
		sadefs.WorkflowType:    request.WorkflowTypeName,
		sadefs.StartTime:       request.StartTime.UnixNano(),
		sadefs.ExecutionTime:   request.ExecutionTime.UnixNano(),
		sadefs.ExecutionStatus: request.Status.String(),
		sadefs.TaskQueue:       request.TaskQueue,
		sadefs.RootWorkflowID:  request.RootWorkflowID,
		sadefs.RootRunID:       request.RootRunID,
	}
	if request.ParentWorkflowID != nil {
		document[sadefs.ParentWorkflowID] = *request.ParentWorkflowID
	}
	if request.ParentRunID != nil {
		document[sadefs.ParentRunID] = *request.ParentRunID
	}
	if len(request.Memo.GetData()) > 0 {
		document[sadefs.Memo] = request.Memo.GetData()
		document[sadefs.MemoEncoding] = request.Memo.GetEncodingType().String()
	}

	searchAttributes, typeMap, err := s.decodeSearchAttributes(request.SearchAttributes)
	if err != nil {
		return nil, err
	}
	textTokens := bson.M{}
	for name, value := range searchAttributes {
		if value == nil {
			continue
		}
		valueType, err := typeMap.GetType(name)
		if err != nil {
			valueType = sadefs.GetMetadataType(request.SearchAttributes.GetIndexedFields()[name])
		}
		if valueType == enumspb.INDEXED_VALUE_TYPE_DATETIME {
			datetime, ok := value.(time.Time)
			if !ok {
				return nil, serviceerror.NewInternalf("search attribute %q has type %T, expected datetime", name, value)
			}
			value = datetime.UnixNano()
		}
		document[name] = value
		if valueType == enumspb.INDEXED_VALUE_TYPE_TEXT {
			textValue, ok := value.(string)
			if !ok {
				return nil, serviceerror.NewInternalf("search attribute %q has type %T, expected string", name, value)
			}
			textTokens[name] = query.TokenizeTextQueryString(textValue)
		}
	}
	if len(textTokens) > 0 {
		document[textTokensField] = textTokens
	}
	return document, nil
}

func (s *VisibilityStore) buildClosedDocument(
	request *store.InternalRecordWorkflowExecutionClosedRequest,
) (bson.M, error) {
	document, err := s.buildDocument(request.InternalVisibilityRequestBase)
	if err != nil {
		return nil, err
	}
	document[closeTimeSortField] = request.CloseTime.UnixNano()
	document[sadefs.CloseTime] = request.CloseTime.UnixNano()
	document[sadefs.ExecutionDuration] = int64(request.ExecutionDuration)
	document[sadefs.HistoryLength] = request.HistoryLength
	document[sadefs.HistorySizeBytes] = request.HistorySizeBytes
	document[sadefs.StateTransitionCount] = request.StateTransitionCount
	return document, nil
}

func tombstoneDocument(request *manager.VisibilityDeleteWorkflowExecutionRequest) bson.M {
	return bson.M{
		documentIDField:    documentID(request.NamespaceID.String(), request.RunID),
		versionField:       request.TaskID,
		deletedField:       true,
		sadefs.NamespaceID: request.NamespaceID.String(),
		sadefs.WorkflowID:  request.WorkflowID,
		sadefs.RunID:       request.RunID,
	}
}

func (s *VisibilityStore) decodeSearchAttributes(
	encoded *commonpb.SearchAttributes,
) (map[string]any, searchattribute.NameTypeMap, error) {
	typeMap, err := s.searchAttributesProvider.GetSearchAttributes(s.indexName, false)
	if err != nil {
		return nil, typeMap, serviceerror.NewUnavailablef("unable to read search attribute types: %v", err)
	}
	decoded, err := searchattribute.Decode(encoded, &typeMap, false)
	if err != nil {
		return nil, typeMap, serviceerror.NewInternalf("unable to decode search attributes: %v", err)
	}
	decoded, err = s.ValidateCustomSearchAttributes(decoded)
	if err != nil {
		var invalidArgument *serviceerror.InvalidArgument
		if !errors.As(err, &invalidArgument) {
			return nil, typeMap, err
		}
	}
	return decoded, typeMap, nil
}

func (s *VisibilityStore) parseDocument(
	document bson.M,
	chasmMapper *chasm.VisibilitySearchAttributesMapper,
) (*store.InternalExecutionInfo, error) {
	typeMap, err := s.searchAttributesProvider.GetSearchAttributes(s.indexName, false)
	if err != nil {
		return nil, serviceerror.NewUnavailablef("unable to read search attribute types: %v", err)
	}
	combinedTypeMap := store.CombineTypeMaps(typeMap, chasmMapper)
	info := &store.InternalExecutionInfo{}
	searchAttributes := make(map[string]any)
	var memo []byte
	var memoEncoding string

	for name, value := range document {
		handled, fieldErr := parseSystemField(info, name, value, &memo, &memoEncoding)
		err = fieldErr
		if !handled {
			var valueType enumspb.IndexedValueType
			valueType, err = combinedTypeMap.GetType(name)
			if err != nil {
				if errors.Is(err, sadefs.ErrInvalidName) {
					err = nil
					continue
				}
				break
			}
			searchAttributes[name], err = normalizeSearchAttributeValue(name, value, valueType)
		}
		if err != nil {
			return nil, serviceerror.NewInternalf("unable to parse MongoDB visibility field %q: %v", name, err)
		}
	}
	if info.ExecutionTime.IsZero() {
		info.ExecutionTime = info.StartTime
	}
	if len(memo) > 0 {
		info.Memo = persistence.NewDataBlob(memo, memoEncoding)
	}
	if len(searchAttributes) > 0 {
		info.SearchAttributes, err = searchattribute.Encode(searchAttributes, &combinedTypeMap)
		if err != nil {
			return nil, serviceerror.NewInternalf("unable to encode search attributes: %v", err)
		}
	}
	return info, nil
}

func parseSystemField(
	info *store.InternalExecutionInfo,
	name string,
	value any,
	memo *[]byte,
	memoEncoding *string,
) (bool, error) {
	var err error
	switch name {
	case documentIDField, versionField, deletedField, closeTimeSortField, textTokensField, sadefs.NamespaceID:
		return true, nil
	case sadefs.WorkflowID:
		info.WorkflowID, err = stringValue(name, value)
	case sadefs.RunID:
		info.RunID, err = stringValue(name, value)
	case sadefs.WorkflowType:
		info.TypeName, err = stringValue(name, value)
	case sadefs.StartTime:
		info.StartTime, err = timeValue(name, value)
	case sadefs.ExecutionTime:
		info.ExecutionTime, err = timeValue(name, value)
	case sadefs.CloseTime:
		info.CloseTime, err = timeValue(name, value)
	case sadefs.ExecutionDuration:
		var duration int64
		duration, err = int64Value(name, value)
		info.ExecutionDuration = time.Duration(duration)
	case sadefs.ExecutionStatus:
		var status string
		status, err = stringValue(name, value)
		if err == nil {
			info.Status, err = enumspb.WorkflowExecutionStatusFromString(status)
		}
	case sadefs.TaskQueue:
		info.TaskQueue, err = stringValue(name, value)
	case sadefs.HistoryLength:
		info.HistoryLength, err = int64Value(name, value)
	case sadefs.HistorySizeBytes:
		info.HistorySizeBytes, err = int64Value(name, value)
	case sadefs.StateTransitionCount:
		info.StateTransitionCount, err = int64Value(name, value)
	case sadefs.ParentWorkflowID:
		info.ParentWorkflowID, err = stringValue(name, value)
	case sadefs.ParentRunID:
		info.ParentRunID, err = stringValue(name, value)
	case sadefs.RootWorkflowID:
		info.RootWorkflowID, err = stringValue(name, value)
	case sadefs.RootRunID:
		info.RootRunID, err = stringValue(name, value)
	case sadefs.Memo:
		*memo, err = bytesValue(name, value)
	case sadefs.MemoEncoding:
		*memoEncoding, err = stringValue(name, value)
	default:
		return false, nil
	}
	return true, err
}

func stringValue(name string, value any) (string, error) {
	result, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("%s has type %T, expected string", name, value)
	}
	return result, nil
}

func timeValue(name string, value any) (time.Time, error) {
	switch typed := value.(type) {
	case time.Time:
		return typed, nil
	case primitive.DateTime:
		return typed.Time(), nil
	case int:
		return time.Unix(0, int64(typed)).UTC(), nil
	case int32:
		return time.Unix(0, int64(typed)).UTC(), nil
	case int64:
		return time.Unix(0, typed).UTC(), nil
	default:
		return time.Time{}, fmt.Errorf("%s has type %T, expected datetime", name, value)
	}
}

func int64Value(name string, value any) (int64, error) {
	switch typed := value.(type) {
	case int:
		return int64(typed), nil
	case int32:
		return int64(typed), nil
	case int64:
		return typed, nil
	default:
		return 0, fmt.Errorf("%s has type %T, expected integer", name, value)
	}
}

func bytesValue(name string, value any) ([]byte, error) {
	switch typed := value.(type) {
	case []byte:
		return typed, nil
	case primitive.Binary:
		return typed.Data, nil
	default:
		return nil, fmt.Errorf("%s has type %T, expected binary", name, value)
	}
}

func normalizeSearchAttributeValue(name string, value any, valueType enumspb.IndexedValueType) (any, error) {
	if valueType == enumspb.INDEXED_VALUE_TYPE_DATETIME {
		return timeValue(name, value)
	}
	if valueType != enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST {
		return value, nil
	}
	switch typed := value.(type) {
	case []string:
		return typed, nil
	case primitive.A:
		result := make([]string, len(typed))
		for index, item := range typed {
			var err error
			result[index], err = stringValue(name, item)
			if err != nil {
				return nil, err
			}
		}
		return result, nil
	case []any:
		result := make([]string, len(typed))
		for index, item := range typed {
			var err error
			result[index], err = stringValue(name, item)
			if err != nil {
				return nil, err
			}
		}
		return result, nil
	default:
		return nil, fmt.Errorf("%s has type %T, expected string list", name, value)
	}
}
