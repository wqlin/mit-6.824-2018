package mapreduce

import (
	"os"
	"encoding/json"
	"sort"
	"log"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	inputFiles := make([] *os.File, nMap)
	for i := 0; i < nMap; i++ {
		fileName := reduceName(jobName, i, reduceTask)
		inputFiles[i], _ = os.Open(fileName)
	}

	// collect key/value pairs from intermediate files
	intermediateKeyValues := make(map[string][]string)
	for _, inputFile := range inputFiles {
		defer inputFile.Close()
		dec := json.NewDecoder(inputFile)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			intermediateKeyValues[kv.Key] = append(intermediateKeyValues[kv.Key], kv.Value)
		}
	}
	keys := make([]string, 0, len(intermediateKeyValues))
	for k := range intermediateKeyValues {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// writer reduce out to file
	out, err := os.Create(outFile)
	if err != nil {
		log.Fatal("Error in creating file", outFile)
	}
	defer out.Close()

	enc := json.NewEncoder(out)
	for _, key := range keys {
		kv := KeyValue{key, reduceF(key, intermediateKeyValues[key])}
		enc.Encode(kv)
	}
}
