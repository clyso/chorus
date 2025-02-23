export default class LocalStorageHelper {
  static get<T>(itemName: string): T | null {
    const item = window.localStorage.getItem(itemName);
    const numPattern = new RegExp(/^\d+$/);
    const jsonPattern = new RegExp(/[[{].*[}\]]/);

    if (!item) {
      return null;
    }

    if (jsonPattern.test(item)) {
      return JSON.parse(item);
    }

    if (numPattern.test(item)) {
      return parseFloat(item) as T;
    }

    return item as T;
  }

  static set<T>(itemName: string, item: T) {
    const stringifiedValue =
      typeof item === 'string' ? item : JSON.stringify(item);

    window.localStorage.setItem(itemName, stringifiedValue);
  }

  static remove(itemName: string) {
    window.localStorage.removeItem(itemName);
  }
}
