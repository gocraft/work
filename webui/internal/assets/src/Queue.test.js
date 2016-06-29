import expect from 'expect';
import Queue from './Queue';
import React from 'react';
import ReactTestUtils from 'react-addons-test-utils';

describe('Queue', () => {
  it('gets queued count', () => {
    let r = ReactTestUtils.createRenderer();
    r.render(<Queue />);
    let queue = r.getMountedInstance();
    expect(queue.state.Queues.length).toEqual(0);

    queue.setState({
      Queues: [
        {JobName: "test", Count: 1, Latency: 0},
        {JobName: "test2", Count: 2, Latency: 0},
      ]
    });

    expect(queue.state.Queues.length).toEqual(2);
    expect(queue.queuedCount).toEqual(3);
  });
});
