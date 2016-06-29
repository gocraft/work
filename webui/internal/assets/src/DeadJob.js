import React from 'react';
import moment from 'moment';
import $ from 'jquery';

export default class DeadJob extends React.Component {
  constructor(props) {
    super(props);

    this.state = {
      Count: 0,
      Jobs: []
    };
  }

  componentWillMount() {
    $.get(this.props.route.source, (data) => {
      this.setState(data);
    });
  }

  render() {
    return (
      <div>
        <h2>Dead Jobs</h2>
        <p>{this.state.Count} job(s) are dead.</p>
        <table>
          <tbody>
            <tr>
              <th>Name</th>
              <th>Arguments</th>
              <th>Error</th>
              <th>Died At</th>
            </tr>
            {
              this.state.Jobs.map((job) => {
                return (
                  <tr>
                    <td>{job.name}</td>
                    <td>{JSON.stringify(job.args)}</td>
                    <td>{job.err}</td>
                    <td>{moment.unix(job.DiedAt).fromNow()}</td>
                  </tr>
                  );
              })
            }
          </tbody>
        </table>
      </div>
    );
  }
}
