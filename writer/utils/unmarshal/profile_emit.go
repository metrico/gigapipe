package unmarshal

import (
	"fmt"

	"github.com/metrico/qryn/v5/writer/model"
)

// emitProfile converts one parsed pprof profile into insert-model rows and
// hands them to the registered profile handler.
//
// Every profile ingest path performs exactly this conversion -- the legacy
// multipart /ingest upload, the raw binary/octet-stream variant and the
// Pyroscope v2 Connect push -- and they differ only in where the application
// name, the tags and the time window come from. Holding the conversion in one
// place means a change to the storage model cannot be applied to two of the
// three paths and forgotten on the third.
func emitProfile(onProfiles onProfileHandler, profile ProfileIR,
	timestampNs, durationNs uint64, name string, tags []model.StrStr) error {
	// Parse always populates this, but the assertion is checked rather than
	// bare: these paths decode unauthenticated request bodies, and a panic in
	// an ingest handler takes down more than the one request that caused it.
	valueAgg, ok := profile.ValueAggregation.([]SampleType)
	if !ok {
		return fmt.Errorf("profile value aggregation is %T, want []SampleType",
			profile.ValueAggregation)
	}

	var sampleUnitArray []model.StrStr
	for i, sType := range profile.Type.SampleType {
		sampleUnitArray = append(sampleUnitArray, model.StrStr{
			Str1: sType,
			Str2: profile.Type.SampleUnit[i],
		})
	}

	var valuesAgg []model.ValuesAgg
	for _, v := range valueAgg {
		valuesAgg = append(valuesAgg, model.ValuesAgg{
			ValueStr:   v.Key,
			ValueInt64: v.Sum,
			ValueInt32: v.Count,
		})
	}

	functions, tree := postProcessProf(profile.Profile)

	var functionArray []model.Function
	for _, f := range functions {
		functionArray = append(functionArray, model.Function{
			ValueInt64: f.ValueInt64,
			ValueStr:   f.ValueStr,
		})
	}

	var treeArray []model.TreeRootStructure
	for _, t := range tree {
		var valuesArr []model.ValuesArrTuple
		for _, v := range t.values {
			valuesArr = append(valuesArr, model.ValuesArrTuple{
				ValueStr:         v.name,
				FirstValueInt64:  v.self,
				SecondValueInt64: v.total,
			})
		}
		treeArray = append(treeArray, model.TreeRootStructure{
			Field1:        t.parentId,
			Field2:        t.funcId,
			Field3:        t.nodeId,
			ValueArrTuple: valuesArr,
		})
	}

	return onProfiles(timestampNs, profile.Type.Type, name, sampleUnitArray,
		profile.Type.PeriodType, profile.Type.PeriodUnit, tags, durationNs,
		fmt.Sprint(profile.PayloadType), profile.Payload.Bytes(),
		valuesAgg, treeArray, functionArray)
}
