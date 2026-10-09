import { describe, expect, it, vi } from 'vitest';
import { docClient, queryAll, queryPage } from '../../../src';

describe('docClient', () => {
  it('is memoized per configuration key', () => {
    expect(docClient('a')).toBe(docClient('a'));
    expect(docClient('a')).not.toBe(docClient('b'));
    expect(docClient()).toBe(docClient('default'));
  });
});

/** A doc client whose Query answers from a list of pages. */
function pagedDoc(pages: Array<{ Items?: unknown[]; LastEvaluatedKey?: Record<string, unknown> }>) {
  let call = 0;
  const send = vi.fn(async () => pages[call++]);
  return { send };
}

describe('queryPage', () => {
  it('continues from a cursor and returns the next one', async () => {
    const doc = pagedDoc([{ Items: [1, 2], LastEvaluatedKey: { pk: 'p', sk: '2' } }]);
    const page = await queryPage(doc, { TableName: 't' }, { pk: 'p', sk: '0' });
    expect(page).toEqual({ items: [1, 2], cursor: { pk: 'p', sk: '2' } });
    const [command] = doc.send.mock.calls[0] as any[];
    expect(command.input).toEqual({ TableName: 't', ExclusiveStartKey: { pk: 'p', sk: '0' } });
  });

  it('treats a missing Items as an empty page', async () => {
    expect(await queryPage(pagedDoc([{}]), { TableName: 't' })).toEqual({ items: [], cursor: undefined });
  });
});

describe('queryAll', () => {
  it('reads until DynamoDB stops handing back a cursor — never just the first 1MB', async () => {
    const doc = pagedDoc([
      { Items: ['a'], LastEvaluatedKey: { k: 1 } },
      { Items: ['b'], LastEvaluatedKey: { k: 2 } },
      { Items: ['c'] },
    ]);
    expect(await queryAll(doc, { TableName: 't' })).toEqual(['a', 'b', 'c']);
    expect(doc.send).toHaveBeenCalledTimes(3);
  });
});
