package mapreduce

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"os"
)

// doMap manages one map task: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	inContent, err := ioutil.ReadFile(inFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}
	keyValue := mapF(inFile, string(inContent))
	partitions := make([]*json.Encoder, nReduce, nReduce)
	for id := 0; id < nReduce; id++ {
		handler, err := os.OpenFile(reduceName(jobName, mapTaskNumber, id), os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return
		}
		defer handler.Close()
		partitions[id] = json.NewEncoder(handler)
	}
	for _, keyValueSingle := range keyValue {
		_ = partitions[ihash(keyValueSingle.Key)%nReduce].Encode(&keyValueSingle)
	}
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}
