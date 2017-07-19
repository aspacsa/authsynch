/*
Purpose:
	Service to read data from flat files.
	Files in multiple paths can be read concurrently.
Usage:
	<Microservice Name> <Name of file containing paths> <brokers host1:9092,host2:9092> <Topic>
Author:
	Ernesto Rodriguez
	aspacsa@gmail.com
*/
package main

import (
	"authsynch/producer/client"
	"authsynch/producer/logtypes"
	"bufio"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sync"
)

var (
	wg sync.WaitGroup
)

/*
	First function to be called.
	Used for initialization only.
*/
func init() {
	logtypes.Info.Println("Initiating...")
}

/*
	Entry point of the service.
	Nothing fancy except that to be able to manage
	multiple paths we are going to use concurrency.
*/
func main() {
	progName := os.Args[0]
	logtypes.Info.Printf("Micro Service: %s\n", progName)
	totArgs := len(os.Args)
	var fileName string
	var brokers string
	var topic string

	if totArgs > 1 {
		fileName = os.Args[1]
		brokers = os.Args[2]
		topic = os.Args[3]
		if fileName == "" {
			log.Fatalln("Must specify the name of file containing paths.")
		}
	} else {
		log.Fatalln(fmt.Sprintf("Usage: %s <name paths file>", progName))
	}

	var paths []string
	paths = read(fileName)

	cpus := runtime.NumCPU()
	logtypes.Info.Printf("Total CPUs: %d\n", cpus)
	runtime.GOMAXPROCS(cpus)
	wg.Add(len(paths))
	logtypes.Info.Println("Processing the following path(s):")
	for _, path := range paths {
		go process(path, brokers, topic)
	}
	wg.Wait()

	logtypes.Info.Println("Finished.")
}

/*
	Read data lines from each flat file in paths specified.
	We will return all lines (from the file) to the caller
	as it is, in other words no special formating will be performed.
*/
func read(fileName string) (lines []string) {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalln("Failed to open file: ", err)
	}

	mylines := make([]string, 1)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if line != "" {
			mylines = append(mylines, line)
		}
	}

	if err := scanner.Err(); err != nil {
		logtypes.Error.Println("Error scanning file.")
	}
	file.Close()

	return mylines
}

/*
	Here we determine if the path to file is valid to
	avoid going to the wrong directory,
	if valid then we read all files in directory
	reading each line in each one of them.
*/
func process(spath string, brokers string, topic string) {
	defer wg.Done()
	logtypes.Info.Println(spath)
	dir := path.Dir(spath)

	if _, err := os.Stat(dir); err == nil {
		files, _ := filepath.Glob(spath)
		for _, file := range files {
			fmt.Println(file)
			for _, line := range read(file) {
				//fmt.Println(line)
				client.Send(brokers, topic, line)
			}
		}
	} else {
		logtypes.Error.Printf("Invalid path '%s'.\n", dir)
	}
}
