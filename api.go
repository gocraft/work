package work

// type Client struct {}
//
//
// func NewClient(namespace string, pool *redis.Pool) *Client {
// 	return nil
// }
//
// // List jobs
// func (c *Client) Jobs() []string {
// 	// todo: how do we know this if we're not connected to a worker?
// 	// opt1: enqueue adds it to <ns>:jobs
// 	// opt2: we list keys on <ns>:jobs:* (using scan?)
// 	// opt3: we don't actually build this command. You configure it.
// 	// opt4: processing a job will add an entry to <ns>:jobs
//	// opt5: we base it on known workerpools and their jobs
// 	return nil
// }
//
// func (c *Client) JobCount(jobName string) int64 {
//
// }
//
// func (c *Client) JobLatency(jobName string) int64 {
//
// }
//
// func (c *Client) DeleteJobs(jobName string) {
// }
//
// func (c *Client) WorkerSetIDs() []string {
//
// }
//
// type WorkerSetStatus struct {
// 	WorkerSetID string
//
// 	WorkerIDs []string
//
// 	Concurrency uint
// 	StartedAt int64
//  HeartbeatAt int64
// 	Jobs []string
// }
//
// func (c *Client) WorkerSetStatuses(workerID []string) []*WorkerSetStatus {
//
// }
//
// type WorkerStatus struct {
// 	WorkerSetID string
// 	WorkerID string
//
// 	IsWorking bool
// 	JobName string
// 	StartedAt int64
// 	Checkin string
// }
//
// func (c *Client) WorkerStatuses(workerID []string) []*WorkerStatus {
//
// }
