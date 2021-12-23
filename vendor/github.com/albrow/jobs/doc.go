// Copyright 2015 Alex Browne.  All rights reserved.
// Use of this source code is governed by the MIT
// license, which can be found in the LICENSE file.

// Package jobs is a persistent and flexible background jobs library.
//
// Version: 0.4.2
//
// Jobs is powered by redis and supports the following features:
//
//   - A job can encapsulate any arbitrary functionality. A job can do anything
//     which can be done in a go function.
//   - A job can be one-off (only executed once) or recurring (scheduled to
//     execute at a specific interval).
//   - A job can be retried a specified number of times if it fails.
//   - A job is persistent, with protections against power loss and other worst
//     case scenarios.
//   - Jobs can be executed by any number of concurrent workers accross any
//     number of machines.
//   - Provided it is persisted to disk, every job will be executed *at least* once,
//	    and in ideal network conditions will be executed *exactly* once.
//   - You can query the database to find out e.g. the number of jobs that are
//     currently executing or how long a particular job took to execute.
//   - Any job that permanently fails will have its error captured and stored.
//
// Why is it Useful
//
// Jobs is intended to be used in web applications. It is useful for cases where you need
// to execute some long-running code, but you don't want your users to wait for the code to
// execute before rendering a response. A good example is sending a welcome email to your users
// after they sign up. You can use Jobs to schedule the email to be sent asynchronously, and
// render a response to your user without waiting for the email to be sent. You could use a
// goroutine to accomplish the same thing, but in the event of a server restart or power loss,
// the email might never be sent. Jobs guarantees that the email will be sent at some time,
// and allows you to spread the work between different machines.
//
//
// More Information
//
// Visit https://github.com/albrow/jobs for a Quickstart Guide, code examples, and more
// information.
//
package jobs
