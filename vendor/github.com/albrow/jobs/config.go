// Copyright 2015 Alex Browne.  All rights reserved.
// Use of this source code is governed by the MIT
// license, which can be found in the LICENSE file.

package jobs

import ()

// configType holds different config variables
type configType struct {
	Db databaseConfig
}

// databaseConfig holds config variables specific to the database
type databaseConfig struct {
	Address  string
	Network  string
	Database int
	Password string
}

// Config is where all configuration variables are stored. You may modify Config
// directly in order to change config variables, and should only do so at the start
// of your program.
var Config = configType{
	Db: databaseConfig{
		// Address is the address of the redis database to connect to. Default is
		// "localhost:6379".
		Address: "localhost:6379",
		// Network is the type of network to use to connect to the redis database
		// Default is "tcp".
		Network: "tcp",
		// Database is the redis database number to use for storing all data. Default
		// is 0.
		Database: 0,
		// Password is a password to use for connecting to a redis database via the
		// AUTH command. If empty, Jobs will not attempt to authenticate. Default is
		// "" (an empty string).
		Password: "",
	},
}
