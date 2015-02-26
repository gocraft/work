package work

// runJob will
// returns an error if the job fails, or there's a panic, or we couldn't reflect correctly.
// if we return an error, it signals we want the job to be retried.
func runJob(job *Job, jt *jobType) error {

	// run middleware

	if jt.IsGeneric {
		err := jt.GenericHandler(job)
		return err
	}

	// given job invoke jt.Handler
	// catch panic

	//
	return nil
}
