import React from 'react';

export default class TruncatedText extends React.Component {
  static propTypes = {
    text: React.PropTypes.string,
    max: React.PropTypes.number,
  }

  render() {
    if (this.props.text.length < this.props.max) {
      return (<span>{this.props.text}</span>);
    } else if (this.props.max <= 2) {
      return (<span>{this.props.text.substring(0, this.props.max)}</span>);
    } else {
      return (<span>{this.props.text.substring(0, this.props.max-2)}..</span>);
    }
  }
}
