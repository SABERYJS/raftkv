package main

import (
	"context"
	"google.golang.org/grpc"
)

func main() {
	//create()
	query()
}

func create() {
	conn, err := grpc.Dial("127.0.0.1:8001", grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	client := NewProtocolsServiceClient(conn)
	ctx := context.Background()
	response, err := client.CreateKey(ctx, &CreateKeyRequest{Data: &KvPair{Key: "name", Value: "martinye"}})
	if err != nil {
		panic(err)
	}
	println(response.Ret, response.KeysCnt)
}

func query() {
	conn, err := grpc.Dial("127.0.0.1:8002", grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	client := NewProtocolsServiceClient(conn)
	ctx := context.Background()
	response, err := client.QueryKey(ctx, &QueryKeyRequest{Key: "name"})
	if err != nil {
		panic(err)
	}
	println(response.Ret, response.Value)
}
