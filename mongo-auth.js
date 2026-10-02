import { initAuthCreds, BufferJSON, makeCacheableSignalKeyStore } from "@kaels/casileys";

export async function useMongoAuthState(db, accountId) {
  const credsCollection = db.collection("whatsapp_auth_creds");
  const keysCollection = db.collection("whatsapp_auth_keys");

  const existingCreds = await credsCollection.findOne({ accountId });

  const creds = existingCreds?.creds
    ? JSON.parse(JSON.stringify(existingCreds.creds), BufferJSON.reviver)
    : initAuthCreds();

  const rawKeys = {
    get: async (type, ids) => {
      const documents = await keysCollection
        .find({
          accountId,
          type,
          id: { $in: ids },
        })
        .toArray();

      const data = {};

      for (const id of ids) {
        const document = documents.find((item) => item.id === id);

        if (document) {
          let value = JSON.parse(
            JSON.stringify(document.value),
            BufferJSON.reviver
          );

          if (type === "app-state-sync-key" && value) {
            try {
              const protoModule = await import("@kaels/casileys");
              const proto = protoModule.proto;

              if (proto?.Message?.AppStateSyncKeyData) {
                value = proto.Message.AppStateSyncKeyData.fromObject(value);
              }
            } catch (error) {
              console.error(
                "⚠️ App-state key conversion warning:",
                error.message
              );
            }
          }

          data[id] = value;
        }
      }

      return data;
    },

    set: async (data) => {
      const operations = [];

      for (const category in data) {
        const categoryData = data[category];

        if (!categoryData) continue;

        for (const id in categoryData) {
          const value = categoryData[id];

          if (value) {
            const serialized = JSON.parse(
              JSON.stringify(value, BufferJSON.replacer)
            );

            operations.push({
              updateOne: {
                filter: {
                  accountId,
                  type: category,
                  id,
                },
                update: {
                  $set: {
                    accountId,
                    type: category,
                    id,
                    value: serialized,
                    updatedAt: new Date(),
                  },
                },
                upsert: true,
              },
            });
          } else {
            operations.push({
              deleteOne: {
                filter: {
                  accountId,
                  type: category,
                  id,
                },
              },
            });
          }
        }
      }

      if (operations.length) {
        await keysCollection.bulkWrite(operations);
      }
    },
  };

  const keys = makeCacheableSignalKeyStore(rawKeys);

  const saveCreds = async () => {
    const serialized = JSON.parse(
      JSON.stringify(creds, BufferJSON.replacer)
    );

    await credsCollection.updateOne(
      { accountId },
      {
        $set: {
          accountId,
          creds: serialized,
          updatedAt: new Date(),
        },
      },
      { upsert: true }
    );
  };

  return {
    state: {
      creds,
      keys,
    },
    saveCreds,
  };
}
