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
	//"authsynch/producer/client"
	"authsynch/producer/logtypes"
	"bufio"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
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
	logtypes.Info.Printf("Micro Service: %s\n", os.Args[0])
	var fileName string
	var brokers string
	var topic string

	if len(os.Args) > 1 {
		fileName = os.Args[1]
		brokers = os.Args[2]
		topic = os.Args[3]
		if fileName == "" {
			log.Fatalln("Must specify the name of file containing paths.")
		}
	} else {
		log.Fatalln(fmt.Sprintf("Usage: %s <name of file containing paths>, <broker1,broker2>, <topic>", os.Args[0]))
	}

	paths := read(fileName)

	start(&paths, brokers, topic)

	logtypes.Info.Println("Finished.")
}

/*
	Here we launch a series of goroutines to process
	all paths containing the targeted files to read from.
*/
func start(paths *[]string, brokers string, topic string) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	wg.Add(len(*paths))
	logtypes.Info.Println("Processing the following path(s):")
	for _, path := range *paths {
		go process(path, brokers, topic)
	}
	wg.Wait()
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
			for idx, line := range read(file) {
				message := formatmsg(formatln(filepath.Base(file), idx+1, &line))
				fmt.Println(*message)
			}
			//client.Send(brokers, topic, line)
		}
	} else {
		logtypes.Error.Printf("Invalid path '%s'.\n", dir)
	}
}

/*
	Format each line read from the file.
*/
func formatln(filename string, idx int, line *string) *string {
	newline := fmt.Sprintf("{\"File\":\"%s\", \"Row\":%s, \"Data\":\"%s\"}", filename, strconv.Itoa(idx), *line)
	return &newline
}

/*
	Package each record in final message format.
*/
func formatmsg(message *string) *string {
	newmsg := fmt.Sprintf("{\"Record\": %s}", *message)
	return &newmsg
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

	mylines := make([]string, 0)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if line != "" {
			mylines = append(mylines, line)
		}
	}

	if err := scanner.Err(); err != nil {
		logtypes.Error.Println("Error reading file.")
	}
	file.Close()

	return mylines
}
