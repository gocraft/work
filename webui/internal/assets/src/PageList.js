import React from 'react';

export default class PageList extends React.Component {

  get totalPage() {
    return Math.ceil(this.props.totalCount / this.props.perPage);
  }

  shouldShow(i) {
    if (i == 1 || i == this.totalPage) {
      return true;
    }
    return Math.abs(this.props.page - i) <= 1;
  }

  render() {
    let pages = [];
    for (let i = 1; i <= this.totalPage; i++) {
      if (this.shouldShow(i)) {
        pages.push(<li onClick={this.props.jumpTo(i)}>{i}</li>);
      } else if (this.shouldShow(i-1)) {
        pages.push(<li>..</li>);
      }
    }
    return (
      <ul>{pages}</ul>
    );
  }
}
