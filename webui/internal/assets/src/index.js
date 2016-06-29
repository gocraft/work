import React from 'react';
import { render } from 'react-dom';
import Process from './Process';
import DeadJob from './DeadJob';
import Queue from './Queue';
import RetryJob from './RetryJob';
import ScheduledJob from './ScheduledJob';
import { Router, Route, Link, IndexRoute, hashHistory } from 'react-router'

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

render(
  <Router history={hashHistory}>
    <Route path="/" component={App}>
      <IndexRoute component={Process}/>
      <Route path="/processes" component={Process} source="/worker_pool"/>
      <Route path="/dead_jobs" component={DeadJob} source="/dead_jobs"/>
      <Route path="/queues" component={Queue} source="/queues"/>
      <Route path="/retry_jobs" component={RetryJob} source="/retry_jobs"/>
      <Route path="/scheduled_jobs" component={ScheduledJob} source="/scheduled_jobs"/>
    </Route>
  </Router>,
  document.getElementById('app')
);
