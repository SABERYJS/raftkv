package main

import (
	"context"
	"google.golang.org/grpc"
)

func main() {
	conn, err := grpc.Dial("127.0.0.1:8001", grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	adminClient := NewRaftAdminClient(conn)
	ctx := context.Background()
	future, err := adminClient.AddVoter(ctx, &AddVoterRequest{Id: "r3", Address: "127.0.0.1:8003", PreviousIndex: 0})
	if err != nil {
		panic(err)
	}
	println(future.GetOperationToken())
}
