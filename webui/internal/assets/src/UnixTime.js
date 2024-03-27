import React from 'react';
import PropTypes from 'prop-types';

export default class UnixTime extends React.Component {
  static propTypes = {
    ts: PropTypes.number.isRequired,
  }

  render() {

    if(this.props.ts === 0 || this.props.ts === '0' || !this.props.ts){
      return null;
    }

    let t = new Date(this.props.ts * 1e3);
    return (
      <time dateTime={t.toLocaleString()}>{t.toLocaleString().slice(0, 19).replace(/-/g, '/').replace('T', ' ')}</time>
    );
  }
}
