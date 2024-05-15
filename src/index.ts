import { Redis } from "ioredis";
import { Prisma } from "@prisma/client/extension";
import { createCache } from "async-cache-dedupe";
import { defaultCacheMethods, defaultMutationMethods } from "./cacheMethods";
import type { CreatePrismaRedisCache, PrismaAction, PrismaMutationAction, PrismaQueryAction } from "./types";

const DEFAULT_CACHE_TIME = 0;

const createPrismaRedisCache = ({
  models,
  onDedupe,
  onError,
  onHit,
  onMiss,
  storage,
  cacheTime = DEFAULT_CACHE_TIME,
  excludeModels = [],
  excludeMethods = [],
  transformer,
  useContainsInvalidation = false,
}: CreatePrismaRedisCache) =>
  Prisma.defineExtension((client) => {
    // Default options for "async-cache-dedupe"
    const cacheOptions = {
      onDedupe,
      onError,
      onHit,
      onMiss,
      storage,
      ttl: cacheTime,
      transformer,
    };

    const cache = createCache(cacheOptions);

    return client.$extends({
      name: "prisma-extension-redis-cache",
      query: {
        async $allOperations({ args: queryArgs, model, operation, query }) {
          type typeQueryArgs = typeof queryArgs;
          let result;

          if (!model) {
            return query(queryArgs);
          }

          // This function is used by `async-cache-dedupe` to fetch data when the cache is empty
          const fetchFromPrisma = async (args: typeof queryArgs) => {
            return query(args);
          };

          // Do not cache any Prisma method specified in the excludeMethods option
          const excludedCacheMethods: PrismaAction[] = defaultCacheMethods.filter((cacheMethod) => {
            return excludeMethods.includes(cacheMethod);
          });

          // Do not cache any Prisma method that has been excluded
          if (!excludedCacheMethods?.includes(operation as PrismaAction)) {
            // Add a cache function for each model specified in the models option
            models?.forEach(({ model: cachedModel, cacheTime, cacheKey, excludeMethods }) => {
              // Only define the cache function for a model if it doesn't exist yet and hasn't been excluded
              if (
                !cache[cachedModel] &&
                cachedModel === model &&
                !excludeModels?.includes(model) &&
                !excludeMethods?.includes(operation as PrismaQueryAction)
              ) {
                cache.define(
                  model,
                  {
                    references: ({ model }: { model: string }, key: string) => {
                      return [`${cacheKey || model}~${key}`];
                    },
                    ttl: cacheTime || cacheOptions.ttl,
                  },
                  async function modelsFetch({
                    cb,
                    args,
                  }: {
                    cb: typeof query;
                    args: typeof queryArgs;
                    queryArgs: typeQueryArgs;
                  }) {
                    result = await cb(args || queryArgs);

                    return result;
                  },
                );
              }
            });

            // For each defined model in `models` we check if they defined any caching methods to be excluded
            const excludedCacheMethodsInModels = models?.find(({ model: cachedModel, excludeMethods }) => {
              return cachedModel === model && excludeMethods?.length;
            });

            // Do not define a cache function for any Prisma model if it already exists
            // Do not define the cache function for a model if it was excluded in `defaultExcludeCacheModels`
            // Do not define a cache function if the Prisma method was exluded in `models`
            if (
              !cache[model] &&
              !excludeModels?.includes(model) &&
              !excludedCacheMethodsInModels?.excludeMethods?.includes(operation as PrismaQueryAction)
            ) {
              cache.define(
                model,
                {
                  references: ({ model }: { model: string }, key: string) => {
                    return [`${model}~${key}`];
                  },
                },
                async function modelFetch({
                  cb,
                  args,
                  queryArgs,
                }: {
                  cb: typeof query;
                  args: typeof queryArgs;
                  queryArgs: typeQueryArgs;
                }) {
                  result = await cb(args || queryArgs);

                  return result;
                },
              );
            }
          }

          // Get the cache function relating to the Prisma model
          const cacheFunction = cache[model];

          // Only cache the data if the Prisma model hasn't been excluded and if the Prisma method wasn't excluded either
          if (
            !excludeModels?.includes(model) &&
            !excludedCacheMethods?.includes(operation as PrismaAction) &&
            !defaultMutationMethods?.includes(operation as PrismaMutationAction) &&
            typeof cacheFunction === "function"
          ) {
            try {
              result = await cacheFunction({ cb: fetchFromPrisma, queryArgs });
            } catch (err) {
              // If we fail to fetch it from the cache (network error, etc.) we will query it from the database
              result = await fetchFromPrisma(queryArgs);

              console.error(err);
            }
          } else {
            const redis = storage?.type === "redis" && storage?.options?.client ? storage.options.client : null;
            // Query the database for any Prisma method (mutation method) or Prisma model we excluded from the cache
            result = await fetchFromPrisma(queryArgs);
            // If we successfully executed the Mutation we clear and invalidate the cache for the Prisma model
            if (defaultMutationMethods.includes(operation as PrismaMutationAction)) {
              const invalidateList = [];
              invalidateList.push(cache.invalidateAll(`*${model}~*`));
              invalidateList.push(cache.invalidateAll(`${model}~*`));
              if (redis) {
                invalidateList.push(removeFormRedis(redis, `*${model}~*`)); //Make sure to delete from redis as well
              }
              if (useContainsInvalidation) {
                invalidateList.push(cache.invalidateAll(`*"${model}":*`));
                invalidateList.push(cache.invalidateAll(`*"${model.toLowerCase()}":*`));
                if (redis) {
                  invalidateList.push(removeFormRedis(redis, `*"${model}":*`)); //Make sure to delete from redis as well
                  invalidateList.push(removeFormRedis(redis, `*"${model.toLowerCase()}":*`)); //Make sure to delete from redis as well
                }
              }
              for (const {
                invalidateRelated,
                customInvalidate,
                useContainsInvalidation: subUseContainsInvalidation,
              } of (models || []).filter((x) => x.model === model)) {
                if (invalidateRelated) {
                  for (const relatedModel of invalidateRelated) {
                    invalidateList.push(cache.invalidateAll(`*${relatedModel}~*`));
                    invalidateList.push(cache.invalidateAll(`${relatedModel}~*`));
                    if (
                      typeof subUseContainsInvalidation !== "undefined"
                        ? subUseContainsInvalidation
                        : useContainsInvalidation
                    ) {
                      invalidateList.push(cache.invalidateAll(`*"${relatedModel}":*`));
                      invalidateList.push(cache.invalidateAll(`*"${relatedModel.toLowerCase()}":*`));
                      if (redis) {
                        invalidateList.push(removeFormRedis(redis, `*"${relatedModel}":*`)); //Make sure to delete from redis as well
                        invalidateList.push(removeFormRedis(redis, `*"${relatedModel.toLowerCase()}":*`)); //Make sure to delete from redis as well
                      }
                    }
                  }
                }
                if (customInvalidate) {
                  for (const customKey of customInvalidate) {
                    invalidateList.push(cache.invalidateAll(customKey));
                    if (redis) {
                      invalidateList.push(removeFormRedis(redis, customKey)); //Make sure to delete from redis as well
                    }
                  }
                }
              }
              await Promise.all(invalidateList);
            }
          }

          return result;
        },
      },
    });
  });

const removeFormRedis = async (redis: Redis, str: string) => {
  if (redis) {
    try {
      // @ts-ignore
      await redis?.del(str);
    } catch (e) {
      console.log("Failed to delete from redis", e);
    }
  }
};

export default createPrismaRedisCache;
