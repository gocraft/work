import React from 'react';
import PageList from './PageList';
import UnixTime from './UnixTime';

export default class DeadJobs extends React.Component {
  static propTypes = {
    fetchURL: React.PropTypes.string,
    deleteURL: React.PropTypes.string,
    retryURL: React.PropTypes.string,
  }

  state = {
    selected: [],
    page: 1,
    Count: 0,
    Jobs: []
  }

  fetch() {
    if (!this.props.fetchURL) {
      return;
    }
    fetch(`${this.props.fetchURL}?page=${this.state.page}`).
      then((resp) => resp.json()).
      then((data) => {
        this.setState({
          selected: [],
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

  checked(job) {
    return this.state.selected.includes(job);
  }

  check(job) {
    var index = this.state.selected.indexOf(job);
    if (index >= 0) {
      this.state.selected.splice(index, 1);
    } else {
      this.state.selected.push(job);
    }
    this.setState({
      selected: this.state.selected
    });
  }

  checkAll() {
    if (this.state.selected.length > 0) {
      this.setState({selected: []});
    } else {
      this.state.Jobs.map((job) => {
        this.state.selected.push(job);
      });
      this.setState({
        selected: this.state.selected
      });
    }
  }

  deleteSelected() {
    let p = [];
    this.state.selected.map((job) => {
      if (!this.props.deleteURL) {
        return;
      }
      p.push(fetch(`${this.props.deleteURL}/${job.DiedAt}/${job.id}`, {method: 'post'}));
    });

    Promise.all(p).then(() => {
      this.fetch();
    });
  }

  retrySelected() {
    let p = [];
    this.state.selected.map((job) => {
      if (!this.props.retryURL) {
        return;
      }
      p.push(fetch(`${this.props.retryURL}/${job.DiedAt}/${job.id}`, {method: 'post'}));
    });

    Promise.all(p).then(() => {
      this.fetch();
    });
  }

  render() {
    return (
      <section>
        <header>Dead Jobs</header>
        <p>{this.state.Count} job(s) are dead.</p>
        <p><PageList page={this.state.page} totalCount={this.state.Count} perPage="20" jumpTo={(page) => () => this.updatePage(page)}/></p>
        <table>
          <tbody>
            <tr>
              <th><input type="checkbox" checked={this.state.selected.length > 0} onChange={() => this.checkAll()}/></th>
              <th>Name</th>
              <th>Arguments</th>
              <th>Error</th>
              <th>Died At</th>
            </tr>
            {
              this.state.Jobs.map((job) => {
                return (
                  <tr key={job.id}>
                    <td><input type="checkbox" checked={this.checked(job)} onChange={() => this.check(job)}/></td>
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
        <p>
          <a onClick={() => this.deleteSelected()}>Delete Selected Jobs</a>
          <a onClick={() => this.retrySelected()}>Retry Selected Jobs</a>
        </p>
      </section>
    );
  }
}
