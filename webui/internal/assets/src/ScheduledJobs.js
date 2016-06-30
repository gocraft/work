import React from 'react';
import PageList from './PageList';

export default class ScheduledJobs extends React.Component {
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
          Jobs: data.Jobs,
        });
      });
  }

  componentWillMount() {
    this.fetch();
  }

  updatePage(page) {
    this.state.page = page;
    this.fetch();
  }

  render() {
    return (
      <section>
        <header>Scheduled Jobs</header>
        <p>{this.state.Count} job(s) scheduled.</p>
        <p><PageList page={this.state.page} totalCount={this.state.Count} perPage="20" jumpTo={(page) => () => this.updatePage(page)}/></p>
        <table>
          <tbody>
            <tr>
              <th>Name</th>
              <th>Arguments</th>
              <th>Scheduled For</th>
            </tr>
            {
              this.state.Jobs.map((job) => {
                return (
                  <tr key={job.id}>
                    <td>{job.name}</td>
                    <td>{JSON.stringify(job.args)}</td>
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
