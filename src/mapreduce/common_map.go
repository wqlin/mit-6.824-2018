package mapreduce

import (
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
	"encoding/json"
)

func doMap(
	jobName string, // the name of the MapReduce job
	mapTask int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(filename string, contents string) []KeyValue,
) {
	file, err := ioutil.ReadFile(inFile)
	if err != nil {
		log.Fatal("doMap: ", err)
	}
	outputFiles := make([] *os.File, nReduce) // create intermediate files
	for i := 0; i < nReduce; i++ {
		fileName := reduceName(jobName, mapTask, i)
		outputFiles[i], err = os.Create(fileName)
		if err != nil {
			log.Fatal("Error in creating file: ", fileName)
		}
	}

	keyValuePairs := mapF(inFile, string(file))
	for _, kv := range keyValuePairs {
		index := ihash(kv.Key) % nReduce
		enc := json.NewEncoder(outputFiles[index])
		enc.Encode(kv)
	}
	for _, file := range outputFiles {
		file.Close()
	}
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}
