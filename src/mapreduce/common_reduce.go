package mapreduce

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	midContentBuf := bytes.NewBuffer(nil)
	for maps := 0; maps < nMap; maps++ {
		f, err := os.Open(reduceName(jobName, maps, reduceTaskNumber))
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return
		}
		io.Copy(midContentBuf, f)
		f.Close()
	}
	midContent := midContentBuf.Bytes()
	reader := bytes.NewReader(midContent)
	decoder := json.NewDecoder(reader)
	var kv KeyValue
	keyValues := make([]KeyValue, 0, 0)
	for {
		err := decoder.Decode(&kv)
		if err == io.EOF {
			break
		}
		keyValues = append(keyValues, kv)
	}
	keyValueMap := make(map[string][]string)
	for _, keyValueSingle := range keyValues {
		keyValueMap[keyValueSingle.Key] = append(keyValueMap[keyValueSingle.Key], keyValueSingle.Value)
	}
	keys := []string{}
	for keyValueSingle := range keyValueMap {
		keys = append(keys, keyValueSingle)
	}
	sort.Strings(keys)
	answerFileName := mergeName(jobName, reduceTaskNumber)
	answerFile, err := os.OpenFile(answerFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}
	encoder := json.NewEncoder(answerFile)
	for _, key := range keys {
		encoder.Encode(KeyValue{key, reduceF(key, keyValueMap[key])})
	}
	answerFile.Close()
}
