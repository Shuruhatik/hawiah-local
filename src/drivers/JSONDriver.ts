import * as fs from 'fs/promises';
import * as path from 'path';
import { Writer } from 'writerx';
import { IDriver, Query, Data } from '../interfaces/IDriver';

/**
 * Driver implementation for storing data in a local JSON file.
 * Stores data as a simple array of objects.
 */
export class JSONDriver implements IDriver {
  private filePath: string;
  private writer: Writer;
  private data: Data[] = [];
  private isConnected: boolean = false;

  /**
   * Creates a new instance of JsonDriver
   * @param filePath - Path to the JSON file
   */
  constructor(filePath: string) {
    this.filePath = path.resolve(filePath);
    this.writer = new Writer(this.filePath);
    this.data = [];
  }

  /**
   * Connects to the database file.
   * Creates the file and directory if they don't exist.
   * Loads existing data into memory.
   */
  async connect(): Promise<void> {
    try {
      const dir = path.dirname(this.filePath);
      await fs.mkdir(dir, { recursive: true });

      // Check if file exists before reading
      try {
        await fs.access(this.filePath);
        const fileContent = await fs.readFile(this.filePath, 'utf-8');
        const parsed = JSON.parse(fileContent);

        if (Array.isArray(parsed)) {
          this.data = parsed;
        } else if (parsed.records && typeof parsed.records === 'object') {
          this.data = Object.values(parsed.records);
        } else {
          this.data = [];
        }
      } catch (error: any) {
        if (error.code === 'ENOENT') {
          this.data = [];
          await this.saveToFile();
        } else {
          throw error;
        }
      }

      this.isConnected = true;
    } catch (error) {
      throw error;
    }
  }

  /**
   * Disconnects from the database.
   */
  async disconnect(): Promise<void> {
    this.isConnected = false;
  }

  /**
   * Saves the data to the JSON file.
   * @private
   */
  private async saveToFile(): Promise<void> {
    const content = JSON.stringify(this.data, null, 2);
    await this.writer.write(content);
  }

  /**
   * Inserts a new record into the database.
   * @param data - The data to insert
   * @returns The inserted record with metadata
   */
  async set(data: Data): Promise<Data> {
    this.ensureConnected();

    const record = {
      ...data,
      _id: data._id || this.generateId(),
      _createdAt: new Date().toISOString(),
      _updatedAt: new Date().toISOString(),
    };

    this.data.push(record);
    await this.saveToFile();

    return record;
  }

  /**
   * Retrieves records matching the query.
   * @param query - The query criteria
   * @returns Array of matching records
   */
  async get(query: Query): Promise<Data[]> {
    this.ensureConnected();

    if (Object.keys(query).length === 0) {
      return [...this.data];
    }

    return this.data.filter(record => this.matchesQuery(record, query));
  }

  /**
   * Retrieves a single record matching the query.
   * @param query - The query criteria
   * @returns The first matching record or null
   */
  async getOne(query: Query): Promise<Data | null> {
    this.ensureConnected();
    const record = this.data.find(record => this.matchesQuery(record, query));
    return record || null;
  }

  /**
   * Updates records matching the query.
   * @param query - The query criteria
   * @param data - The data to update
   * @returns The number of updated records
   */
  async update(query: Query, data: Data): Promise<number> {
    this.ensureConnected();
    let count = 0;

    this.data = this.data.map(record => {
      if (this.matchesQuery(record, query)) {
        count++;
        return {
          ...record,
          ...data,
          _updatedAt: new Date().toISOString(),
        };
      }
      return record;
    });

    if (count > 0) {
      await this.saveToFile();
    }

    return count;
  }

  /**
   * Deletes records matching the query.
   * @param query - The query criteria
   * @returns The number of deleted records
   */
  async delete(query: Query): Promise<number> {
    this.ensureConnected();
    const initialLength = this.data.length;
    this.data = this.data.filter(record => !this.matchesQuery(record, query));
    const count = initialLength - this.data.length;

    if (count > 0) {
      await this.saveToFile();
    }

    return count;
  }

  /**
   * Checks if any record matches the query.
   * @param query - The query criteria
   * @returns True if a match exists, false otherwise
   */
  async exists(query: Query): Promise<boolean> {
    this.ensureConnected();
    return this.data.some(record => this.matchesQuery(record, query));
  }

  /**
   * Counts records matching the query.
   * @param query - The query criteria
   * @returns The number of matching records
   */
  async count(query: Query): Promise<number> {
    this.ensureConnected();
    if (Object.keys(query).length === 0) {
      return this.data.length;
    }
    return this.data.filter(record => this.matchesQuery(record, query)).length;
  }

  /**
   * Manually saves data to file.
   */
  async save(): Promise<void> {
    this.ensureConnected();
    await this.saveToFile();
  }

  /**
   * Clears all data from the database.
   */
  async clear(): Promise<void> {
    this.ensureConnected();
    this.data = [];
    await this.saveToFile();
  }

  /**
   * Deletes the JSON file.
   */
  async drop(): Promise<void> {
    this.ensureConnected();
    try {
      await fs.unlink(this.filePath);
    } catch (error: any) {
      if (error.code !== 'ENOENT') {
        throw error;
      }
    }
    this.data = [];
  }

  /**
   * Reloads data from the JSON file.
   */
  async reload(): Promise<void> {
    this.ensureConnected();
    try {
      const fileContent = await fs.readFile(this.filePath, 'utf-8');
      const parsed = JSON.parse(fileContent);
      if (Array.isArray(parsed)) {
        this.data = parsed;
      } else {
        this.data = [];
      }
    } catch (error: any) {
      if (error.code === 'ENOENT') {
        this.data = [];
      } else {
        throw error;
      }
    }
  }

  /**
   * Gets all data in memory.
   * @returns All records
   */
  getAllData(): Data[] {
    return [...this.data];
  }

  /**
   * Gets the JSON file path.
   * @returns The file path
   */
  getFilePath(): string {
    return this.filePath;
  }

  /**
   * Ensures the driver is connected before executing operations.
   * @throws Error if driver is not connected
   * @private
   */
  private ensureConnected(): void {
    if (!this.isConnected) {
      throw new Error('Driver not connected. Call connect() first.');
    }
  }

  /**
   * Checks if a record matches the query criteria.
   * Supports nested objects and exact array matching.
   * @param record - The record to check
   * @param query - The query criteria
   * @returns True if the record matches, false otherwise
   */
  private matchesQuery(record: Data, query: Query): boolean {
    if (Object.keys(query).length === 0) {
      return true;
    }

    return Object.keys(query).every(key => {
      const queryValue = query[key];
      const recordValue = record[key];

      if (typeof queryValue === 'object' && queryValue !== null && !Array.isArray(queryValue)) {
        if (typeof recordValue === 'object' && recordValue !== null && !Array.isArray(recordValue)) {
          return this.matchesQuery(recordValue, queryValue);
        }
        return false;
      }

      if (Array.isArray(queryValue) && Array.isArray(recordValue)) {
        return JSON.stringify(queryValue) === JSON.stringify(recordValue);
      }

      return recordValue === queryValue;
    });
  }

  /**
   * Generates a unique ID for records.
   * @returns A unique string ID
   * @private
   */
  private generateId(): string {
    return `${Date.now()}_${Math.random().toString(36).substring(2, 15)}`;
  }
}
