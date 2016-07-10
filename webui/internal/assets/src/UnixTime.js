import React from 'react';

export default class UnixTime extends React.Component {
  static propTypes = {
    ts: React.PropTypes.number.isRequired,
  }

  render() {
    let t = new Date(this.props.ts * 1e3);
    return (
      <time dateTime={t.toISOString()}>{t.toISOString().slice(0, 19).replace(/-/g, '/').replace('T', ' ')}</time>
    );
  }
}
