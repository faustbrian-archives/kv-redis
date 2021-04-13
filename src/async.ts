import { IKeyValueStoreAsync } from "@konceiver/kv";
import Redis from "ioredis";

export class StoreAsync<K, T> implements IKeyValueStoreAsync<K, T> {
	public static async new<K, T>(): Promise<StoreAsync<K, T>> {
		return new StoreAsync<K, T>(new Redis());
	}

	private constructor(private readonly store) {}

	public async all(): Promise<Array<[K, T]>> {
		const keys: K[] = await this.keys();

		// @ts-ignore
		return Promise.all(
			keys.map(async (key) => [key, await this.store.get(key)])
		);
	}

	public async keys(): Promise<K[]> {
		return this.store.keys("*");
	}

	public async values(): Promise<T[]> {
		const keys: K[] = await this.keys();

		return Promise.all(keys.map(async (key: K) => this.store.get(key)));
	}

	public async get(key: K): Promise<T | undefined> {
		return (await this.store.get(key)) || undefined;
	}

	public async getMany(keys: K[]): Promise<Array<T | undefined>> {
		return Promise.all([...keys].map(async (key: K) => this.get(key)));
	}

	public async pull(key: K): Promise<T | undefined> {
		const item: T | undefined = await this.get(key);

		await this.forget(key);

		return item;
	}

	public async pullMany(keys: K[]): Promise<Array<T | undefined>> {
		const items: Array<T | undefined> = await this.getMany(keys);

		await this.forgetMany(keys);

		return items;
	}

	public async put(key: K, value: T): Promise<boolean> {
		await this.store.set(key, value);

		return this.has(key);
	}

	public async putMany(values: Array<[K, T]>): Promise<boolean[]> {
		return Promise.all(
			values.map(async (value: [K, T]) => this.put(value[0], value[1]))
		);
	}

	public async has(key: K): Promise<boolean> {
		return (await this.get(key)) !== undefined;
	}

	public async hasMany(keys: K[]): Promise<boolean[]> {
		return Promise.all([...keys].map(async (key: K) => this.has(key)));
	}

	public async missing(key: K): Promise<boolean> {
		return !(await this.has(key));
	}

	public async missingMany(keys: K[]): Promise<boolean[]> {
		return Promise.all([...keys].map(async (key: K) => this.missing(key)));
	}

	public async forget(key: K): Promise<boolean> {
		if (await this.missing(key)) {
			return false;
		}

		await this.store.del(key);

		return this.missing(key);
	}

	public async forgetMany(keys: K[]): Promise<boolean[]> {
		return Promise.all([...keys].map((key: K) => this.forget(key)));
	}

	public async flush(): Promise<boolean> {
		await this.store.flushall();

		return this.isEmpty();
	}

	public async count(): Promise<number> {
		return (await this.store.keys("*")).length;
	}

	public async isEmpty(): Promise<boolean> {
		return (await this.count()) === 0;
	}

	public async isNotEmpty(): Promise<boolean> {
		return !(await this.isEmpty());
	}
}
