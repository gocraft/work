import React from 'react';
import { render } from 'react-dom';
import Process from './Process';
import DeadJob from './DeadJob';
import Queue from './Queue';
import RetryJob from './RetryJob';
import ScheduledJob from './ScheduledJob';
import { Router, Route, Link, IndexRoute, hashHistory, Redirect } from 'react-router'

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
      <Route path="/processes" component={ () => <Process url="/worker_pool" /> } />
      <Route path="/dead_jobs" component={ () => <DeadJob url="/dead_jobs" /> } />
      <Route path="/queues" component={ () => <Queue url="/queues" /> } />
      <Route path="/retry_jobs" component={ () => <RetryJob url="/retry_jobs" /> } />
      <Route path="/scheduled_jobs" component={ () => <ScheduledJob url="/scheduled_jobs" /> } />
      <Redirect from="*" to="/queues" />
    </Route>
  </Router>,
  document.getElementById('app')
);
