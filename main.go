package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/glacier"
)

var PartSize = "1048576"
var AccountID = "-"
var VaultName = "pop-os"
var ArchiveDescription = "my pc backup"

func main() {
	channel := make(chan []byte)

	context, cancel := context.WithCancel(context.Background())
	defer cancel()

	awsConfig, err := config.LoadDefaultConfig(context)
	if err != nil {
		panic(err)
	}

	awsClient := glacier.NewFromConfig(awsConfig)

	go WalkThroughFiles(context, channel, awsClient)

	go SendToGlacier(context, channel, awsClient)

	interruptWatcher := make(chan os.Signal)
	signal.Notify(interruptWatcher, os.Interrupt)

	go func() {
		select {
		case signal := <-interruptWatcher:
			fmt.Println("Got %s signal. Aborting...\n", signal)
			os.Exit(1)
		}
	}()
}

func WalkThroughFiles(ctx context.Context, ch chan<- []byte, awsClient *glacier.Client) {
	err := filepath.Walk("$HOME", func(path string, info os.FileInfo, err error) error {
		ArchiveDescription = fmt.Sprintf("%s", path)
		initiateParams := &glacier.InitiateMultipartUploadInput{
			AccountId:          &AccountID,
			VaultName:          &VaultName,
			ArchiveDescription: &ArchiveDescription,
			PartSize:           &PartSize,
		}

		awsClient.InitiateMultipartUpload(ctx, initiateParams)
		ReadFromFile(ctx, path, ch)
		return nil
	})
	if err != nil {
		panic(err)
	}

}

func SendToGlacier(ctx context.Context, ch <-chan []byte, awsClient *glacier.Client) {

	for {
		readFromChannel := <-ch

		reader := bytes.NewReader(readFromChannel)

		checksumArray := sha256.Sum256(readFromChannel)

		checksumSlice := []byte(checksumArray[:])

		checksumString := hex.EncodeToString(checksumSlice)

		lengthInt := len(<-ch)

		length := string(lengthInt)

		var UploadPartParams = &glacier.UploadMultipartPartInput{
			AccountId: &AccountID,
			VaultName: &VaultName,
			Body:      reader,
			Checksum:  &checksumString,
			Range:     &length,
		}

		_, err := awsClient.UploadMultipartPart(ctx, UploadPartParams)
		if err != nil {
			panic(err)
		}
	}
}

func ReadFromFile(ctx context.Context, path string, ch chan<- []byte) {
	file, err := os.Open(path)
	if err != nil {
		panic(err)
	}

	const chunkSize = 11048576

	buffer := make([]byte, chunkSize)

	for {
		readTotal, err := file.Read(buffer)
		if err != nil {
			if err != io.EOF {
				panic(err)
			}
			break
		}
		ch <- buffer[:readTotal]
	}
}
