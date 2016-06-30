import expect from 'expect';
import Queues from './Queues';
import React from 'react';
import ReactTestUtils from 'react-addons-test-utils';

describe('Queues', () => {
  it('gets queued count', () => {
    let r = ReactTestUtils.createRenderer();
    r.render(<Queues />);
    let queues = r.getMountedInstance();
    expect(queues.state.Queues.length).toEqual(0);

    queues.setState({
      Queues: [
        {JobName: 'test', Count: 1, Latency: 0},
        {JobName: 'test2', Count: 2, Latency: 0}
      ]
    });

    expect(queues.state.Queues.length).toEqual(2);
    expect(queues.queuedCount).toEqual(3);
  });
});
