import React from 'react';
import PageList from './PageList';
import UnixTime from './UnixTime';

export default class RetryJobs extends React.Component {
  constructor() {
    super();

    this.state = {
      page: 1,
      Count: 0,
      Jobs: []
    };
  }

  fetch() {
    if (!this.props.url) {
      return;
    }
    fetch(`${this.props.url}?page=${this.state.page}`).
      then((resp) => resp.json()).
      then((data) => {
        this.setState({
          Count: data.Count,
          Jobs: data.Jobs
        });
      });
  }

  componentWillMount() {
    this.fetch();
  }

  updatePage(page) {
    this.setState({page: page});
    this.fetch();
  }

  render() {
    return (
      <section>
        <header>Retry Jobs</header>
        <p>{this.state.Count} job(s) scheduled to be retried.</p>
        <p><PageList page={this.state.page} totalCount={this.state.Count} perPage="20" jumpTo={(page) => () => this.updatePage(page)}/></p>
        <table>
          <tbody>
            <tr>
              <th>Name</th>
              <th>Arguments</th>
              <th>Error</th>
              <th>Retry At</th>
            </tr>
            {
              this.state.Jobs.map((job) => {
                return (
                  <tr key={job.id}>
                    <td>{job.name}</td>
                    <td>{JSON.stringify(job.args)}</td>
                    <td>{job.err}</td>
                    <td><UnixTime ts={job.t} /></td>
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
