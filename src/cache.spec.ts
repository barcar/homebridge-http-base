import {Cache} from './cache';

let cache: Cache;

describe("Cache", function () {
    beforeEach(function () {
        jest.useFakeTimers();
        jest.setSystemTime(new Date('2024-01-01T00:00:00Z'));
        cache = new Cache(1);
    });

    afterEach(function () {
        jest.useRealTimers();
    });

    it('should return shouldQuery=true on first call', function () {
        expect(cache.shouldQuery()).toBe(true);
    });
    it('should return shouldQuery=false shortly after last query', function () {
        cache.queried();
        expect(cache.shouldQuery()).toBe(false);
    });

    it('should return shouldQuery=true after cache time expired', function () {
        cache.queried();
        jest.advanceTimersByTime(1);
        expect(cache.shouldQuery()).toBe(false);
        jest.advanceTimersByTime(1);
        expect(cache.shouldQuery()).toBe(true);
    });

    it('should return shouldQuery=false when caching infinitely', function () {
        const cache = new Cache(-1);
        expect(cache.shouldQuery()).toBe(false);

        cache.queried();
        expect(cache.shouldQuery()).toBe(false);
        expect(cache.isInfinite()).toBe(true);
    });
});
