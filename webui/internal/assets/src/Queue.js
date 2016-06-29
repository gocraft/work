import React from 'react';
import $ from 'jquery';

export default class Queue extends React.Component {
  constructor(props) {
    super(props);

    this.state = {
      Queues: []
    };
  }

  componentWillMount() {
    if (!this.props.url) {
      return;
    }
    $.get(this.props.url, (data) => {
      this.setState({Queues: data});
    });
  }

  get queuedCount() {
    let count = 0;
    this.state.Queues.map((queue) => {
      count += queue.Count;
    })
    return count;
  }

  render() {
    return (
      <section>
        <header>Queues</header>
        <p>{this.state.Queues.length} queue(s) with a total of {this.queuedCount} item(s) queued.</p>
        <table>
          <tbody>
            <tr>
              <th>Name</th>
              <th>Count</th>
              <th>Latency (seconds)</th>
            </tr>
            {
              this.state.Queues.map((queue) => {
                return (
                  <tr>
                    <td>{queue.JobName}</td>
                    <td>{queue.Count}</td>
                    <td>{queue.Latency}</td>
                  </tr>
                  );
              })
            }
          </tbody>
        </table>
      </section>
    );
  }
}
