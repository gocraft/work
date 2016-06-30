import React from 'react';

export default class UnixTime extends React.Component {
  render() {
    let t = new Date(this.props.ts * 1e3);
    return (
      <time datetime={t.toISOString()}>{t.toLocaleString()}</time>
    );
  }
}
