jest.unmock('../DeadJob');

import DeadJob from '../DeadJob';

describe('sum', () => {
  it('adds 1 + 2 to equal 3', () => {
    const sum = (a, b) => a+b;
    expect(sum(1, 2)).toBe(3);
  });
});
