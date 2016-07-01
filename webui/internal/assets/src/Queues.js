import React from 'react';
import styles from './css/bootstrap.min.css';
import cx from './cx';

export default class Queues extends React.Component {
  static propTypes = {
    url: React.PropTypes.string,
  }

  state = {
    Queues: []
  }

  componentWillMount() {
    if (!this.props.url) {
      return;
    }
    fetch(this.props.url).
      then((resp) => resp.json()).
      then((data) => {
        this.setState({Queues: data});
      });
  }

  get queuedCount() {
    let count = 0;
    this.state.Queues.map((queue) => {
      count += queue.Count;
    });
    return count;
  }

  render() {
    return (
      <div className={cx(styles.panel, styles.panelDefault)}>
        <div className={styles.panelHeading}>Queues</div>
        <div className={styles.panelBody}>
          <p>{this.state.Queues.length} queue(s) with a total of {this.queuedCount} item(s) queued.</p>
        </div>
        <table className={styles.table}>
          <tbody>
            <tr>
              <th>Name</th>
              <th>Count</th>
              <th>Latency (seconds)</th>
            </tr>
            {
              this.state.Queues.map((queue) => {
                return (
                  <tr key={queue.JobName}>
                    <td>{queue.JobName}</td>
                    <td>{queue.Count}</td>
                    <td>{queue.Latency}</td>
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
