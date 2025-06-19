import { config } from "dotenv";
import { Camunda8 } from "@camunda8/sdk";

config();

const camunda = new Camunda8({
  ZEEBE_ADDRESS: process.env.ZEEBE_ADDRESS!,
  ZEEBE_CLIENT_ID: process.env.ZEEBE_CLIENT_ID!,
  ZEEBE_CLIENT_SECRET: process.env.ZEEBE_CLIENT_SECRET!,
  CAMUNDA_OAUTH_URL: process.env.CAMUNDA_OAUTH_URL!,
  CAMUNDA_AUTH_STRATEGY: "OAUTH",
});
const zeebe = camunda.getZeebeGrpcApiClient();

// ---- Worker: sendRejection (Send Task) ----
zeebe.createWorker({
  taskType: "sendRejection",
  taskHandler: async (job) => {
    const { customerID } = job.variables;
    await zeebe.publishMessage({
      name: "Message_0mrwobu",
      correlationKey: String(customerID),
      timeToLive: 0,
      variables: { rejectionSent: true },
    });
    return job.complete();
  },
});

// ---- Worker: AntragUnvollstaendigNachrichtVersenden (Intermediate Throw) ----
zeebe.createWorker({
  taskType: "AntragUnvollstaendigNachrichtVersenden",
  taskHandler: async (job) => {
    const { customerID } = job.variables;
    await zeebe.publishMessage({
      name: "Message_AntragOnline",
      correlationKey: String(customerID),
      timeToLive: 0,
      variables: { reminderSent: true },
    });
    return job.complete();
  },
});

// ---- Worker: AbsageNachrichtVersenden (Message End Event) ----
zeebe.createWorker({
  taskType: "AbsageNachrichtVersenden",
  taskHandler: async (job) => {
    return job.complete();
  },
});

// ---- Worker: vertragErstellen (Service Task) ----
zeebe.createWorker({
  taskType: "vertragErstellen",
  taskHandler: async (job) => {
    return job.complete();
  },
});

// ---- Worker: ZusageNachrichtVersenden (Message End Event) ----
zeebe.createWorker({
  taskType: "ZusageNachrichtVersenden",
  taskHandler: async (job) => {
    return job.complete();
  },
});

// ---- Worker: stornierungsbestätigung (Message End Event) ----
zeebe.createWorker({
  taskType: "stornierungsbestätigung",
  taskHandler: async (job) => {
    return job.complete();
  },
});

console.log("Camunda 8 JS SDK Zeebe Workers gestartet.");
