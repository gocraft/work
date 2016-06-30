import React from 'react';
import { render } from 'react-dom';
import Processes from './Processes';
import DeadJobs from './DeadJobs';
import Queues from './Queues';
import RetryJobs from './RetryJobs';
import ScheduledJobs from './ScheduledJobs';
import { Router, Route, Link, IndexRedirect, hashHistory } from 'react-router'

class App extends React.Component {
  render() {
    return (
      <main>
        <header><h1>gocraft/work</h1></header>
        <nav>
          <ul>
            <li><Link to="/processes">Processes</Link></li>
            <li><Link to="/dead_jobs">Dead Jobs</Link></li>
            <li><Link to="/queues">Queues</Link></li>
            <li><Link to="/retry_jobs">Retry Jobs</Link></li>
            <li><Link to="/scheduled_jobs">Scheduled Jobs</Link></li>
          </ul>
        </nav>
        {this.props.children}
      </main>
    );
  }
}

// react-router's route cannot be used to specify props to children component.
// See https://github.com/reactjs/react-router/issues/1857.
render(
  <Router history={hashHistory}>
    <Route path="/" component={App}>
      <Route path="/processes" component={ () => <Processes busyWorkerURL="/busy_workers" workerPoolURL="/worker_pools" /> } />
      <Route path="/dead_jobs" component={ () => <DeadJobs fetchURL="/dead_jobs" retryURL="/retry_dead_job" deleteURL="/delete_dead_job" /> } />
      <Route path="/queues" component={ () => <Queues url="/queues" /> } />
      <Route path="/retry_jobs" component={ () => <RetryJobs url="/retry_jobs" /> } />
      <Route path="/scheduled_jobs" component={ () => <ScheduledJobs url="/scheduled_jobs" /> } />
      <IndexRedirect from="" to="/processes" />
    </Route>
  </Router>,
  document.getElementById('app')
);
