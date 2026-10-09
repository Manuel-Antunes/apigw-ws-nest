/* =============================================================================
 *  FakeDynamo — an in-memory DynamoDB DocumentClient for the aws-mode classes.
 * =============================================================================
 *  The library builds REAL @aws-sdk/lib-dynamodb commands and hands them to
 *  docClient().send(); tests replace docClient() (vi.mock of
 *  src/providers/dynamo) with this, so what is asserted is the item shape and
 *  the query semantics the library relies on — not a mock's call log.
 *
 *  Implements exactly what the library uses: Put (with attribute_not_exists),
 *  Get, Delete, and Query on `pk = :pk` / `pk = :pk AND begins_with(sk, :prefix)`
 *  with sk ordering and paging. Like the real DocumentClient, a Put carrying an
 *  `undefined` attribute throws unless the client was created with
 *  `marshallOptions.removeUndefinedValues`.
 * ========================================================================== */

type Item = Record<string, any>;

export interface FakeCall {
  command: string;
  input: any;
}

const keyOf = (item: Item) => `${item.pk}\u0000${item.sk}`;

function hasUndefined(value: unknown): boolean {
  if (value === undefined) return true;
  if (Array.isArray(value)) return value.some(hasUndefined);
  if (value && typeof value === 'object') return Object.values(value).some(hasUndefined);
  return false;
}

function stripUndefined<T>(value: T): T {
  return JSON.parse(JSON.stringify(value));
}

export class FakeDynamo {
  readonly tables = new Map<string, Map<string, Item>>();
  readonly calls: FakeCall[] = [];
  /** Items per Query page. */
  pageSize = Number.POSITIVE_INFINITY;
  /** Return an error to make a command fail. */
  failWith?: (command: string, input: any) => Error | undefined;

  reset() {
    this.tables.clear();
    this.calls.length = 0;
    this.pageSize = Number.POSITIVE_INFINITY;
    this.failWith = undefined;
  }

  /** What docClient(key, options) returns. */
  client(options: { marshallOptions?: { removeUndefinedValues?: boolean } } = {}) {
    return { send: (command: any) => this.send(command, options) };
  }

  table(name: string): Map<string, Item> {
    let table = this.tables.get(name);
    if (!table) this.tables.set(name, (table = new Map()));
    return table;
  }

  /** Every item of a table, sorted by key. */
  items(name: string): Item[] {
    return [...this.table(name).values()].sort((a, b) => keyOf(a).localeCompare(keyOf(b)));
  }

  /** Seed an item directly. */
  seed(tableName: string, item: Item) {
    this.table(tableName).set(keyOf(item), structuredClone(item));
  }

  private async send(command: any, options: { marshallOptions?: { removeUndefinedValues?: boolean } }) {
    const name: string = command.constructor.name;
    const input = command.input;
    this.calls.push({ command: name, input });
    const failure = this.failWith?.(name, input);
    if (failure) throw failure;
    switch (name) {
      case 'PutCommand':
        return this.put(input, !!options.marshallOptions?.removeUndefinedValues);
      case 'GetCommand':
        return this.get(input);
      case 'DeleteCommand':
        return this.delete(input);
      case 'QueryCommand':
        return this.query(input);
      default:
        throw new Error(`FakeDynamo: ${name} is not implemented`);
    }
  }

  private put(input: any, removeUndefined: boolean) {
    if (hasUndefined(input.Item) && !removeUndefined) {
      throw new Error(
        'Pass options.removeUndefinedValues=true to remove undefined values from map/array/set.',
      );
    }
    const table = this.table(input.TableName);
    const item = stripUndefined(input.Item);
    if (input.ConditionExpression) {
      if (input.ConditionExpression !== 'attribute_not_exists(pk)') {
        throw new Error(`FakeDynamo: condition ${input.ConditionExpression} is not implemented`);
      }
      if (table.has(keyOf(item))) {
        const err = new Error('The conditional request failed');
        err.name = 'ConditionalCheckFailedException';
        throw err;
      }
    }
    table.set(keyOf(item), item);
    return {};
  }

  private get(input: any) {
    const item = this.table(input.TableName).get(keyOf(input.Key));
    return { Item: item ? structuredClone(item) : undefined };
  }

  private delete(input: any) {
    this.table(input.TableName).delete(keyOf(input.Key));
    return {};
  }

  private query(input: any) {
    const expr: string = input.KeyConditionExpression;
    const values = input.ExpressionAttributeValues ?? {};
    let prefix: string | undefined;
    if (expr === 'pk = :pk AND begins_with(sk, :prefix)') prefix = values[':prefix'];
    else if (expr !== 'pk = :pk') throw new Error(`FakeDynamo: query ${expr} is not implemented`);

    const matching = this.items(input.TableName).filter(
      item => item.pk === values[':pk'] && (prefix === undefined || String(item.sk).startsWith(prefix)),
    );
    const start = input.ExclusiveStartKey
      ? matching.findIndex(item => keyOf(item) === keyOf(input.ExclusiveStartKey)) + 1
      : 0;
    const page = matching.slice(start, start + this.pageSize);
    const last = page[page.length - 1];
    const more = start + page.length < matching.length;
    return {
      Items: page.map(item => structuredClone(item)),
      ...(more && last ? { LastEvaluatedKey: { pk: last.pk, sk: last.sk } } : {}),
    };
  }
}

/** The one instance a test file's vi.mock factory hands out. */
export const fakeDynamo = new FakeDynamo();
