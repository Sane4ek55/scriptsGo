package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"strings"
)


const (
	topicName     = ""
	consumerGroup = ""
	kafkaBrokers = ""
)

var (
	lag    int64 = 0
	sumLag int64 = 0
)

func main() {

	lagByGroupAndTopic, err := GetLag()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("lagByGroupAndTopic :", lagByGroupAndTopic)
	for num, _ := range lagByGroupAndTopic {

		sumLag += lagByGroupAndTopic[num]
	}
	fmt.Println("sumLag :", sumLag)

}

func GetLag() (map[int32]int64, error) {
	cfg := sarama.NewConfig()
	brokers := strings.Split(kafkaBrokers, ",")
	client, err := sarama.NewClient(brokers, cfg)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		return nil, err
	}
	defer admin.Close()


	if err = client.RefreshMetadata(topicName); err != nil {
		fmt.Println(err)
	}

	var groupOffsets *sarama.OffsetFetchResponse
	groupOffsets, err = admin.ListConsumerGroupOffsets(consumerGroup, nil)
	if err != nil {
		fmt.Println(err)
	}
	var partitions []int32
	partitions, err = client.Partitions(topicName)
	if err != nil {
		fmt.Println(err)
	}

	lagByPartition := make(map[int32]int64)

	for _, partition := range partitions {
		block, ok := groupOffsets.Blocks[topicName][partition]
		if !ok {
			fmt.Println(err)
			continue
		}
		var offset int64
		offset, err = client.GetOffset(topicName, partition, sarama.OffsetNewest)
		if err != nil {
			fmt.Println(err)
			continue
		}

		groupOffset := block.Offset
		lag = offset - groupOffset
		lagByPartition[partition] = lag
	}


	return lagByPartition, nil
}
