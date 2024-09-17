#!/usr/bin/env node

import {WebSocketServer} from 'ws'
import http from 'http'
import * as map from 'lib0/map'

const wsReadyStateConnecting = 0;
const wsReadyStateOpen = 1;
const wsReadyStateClosing = 2; // eslint-disable-line
const wsReadyStateClosed = 3; // eslint-disable-line

const pingTimeout = 30000;

const port = process.env.PORT || 4444;
const wss = new WebSocketServer({noServer: true});

const server = http.createServer((request, response) => {
    response.writeHead(200, {'Content-Type': 'text/plain'});
    response.end('okay')
});

/**
 * Map froms topic-name to set of subscribed clients.
 * @type {Map<string, Set<any>>}
 */
const topics = new Map();

function send(conn, message) {
    if (conn.readyState !== wsReadyStateConnecting && conn.readyState !== wsReadyStateOpen) {
        conn.close()
    }
    try {
        conn.send(JSON.stringify(message))
    } catch (e) {
        console.log(e);
        conn.close()
    }
}

function onconnection(conn) {
    /**
     * @type {Set<string>}
     */
    const subscribedTopics = new Set();
    let closed = false;

    let pongReceived = true;
    const pingInterval = setInterval(() => {
        if (!pongReceived) {
            conn.close();
            clearInterval(pingInterval);
            return;
        }
        pongReceived = false;
        try {
            conn.ping()
        } catch (e) {
            console.log(e);
            conn.close()
        }
    }, pingTimeout);

    conn.on('pong', () => {
        pongReceived = true
    });

    conn.on('close', () => {
        for (const topicName of subscribedTopics) {
            const subs = topics.get(topicName) ?? new Set();
            subs.delete(conn);
            if (subs.size === 0) topics.delete(topicName)
        }
        subscribedTopics.clear();
        closed = true
    });

    conn.on('message', message => {
        if (typeof message === 'string' || message instanceof Buffer) {
            message = JSON.parse(message)
        }
        if (message && message.type && !closed) {
            const topicNames = message.topics ?? [];
            switch (message.type) {
                case 'subscribe':
                    for (const topicName of topicNames) {
                        if (typeof topicName === 'string') {
                            const topic = map.setIfUndefined(topics, topicName, () => new Set());
                            topic.add(conn);
                            subscribedTopics.add(topicName)
                        }
                    }
                    break;
                case 'unsubscribe':
                    for (const topicName of topicNames) {
                        const subs = topics.get(topicName);
                        if (subs) subs.delete(conn)
                    }
                    break;
                case 'publish':
                    if (message.topic) {
                        const receivers = topics.get(message.topic);
                        if (receivers) {
                            message.clients = receivers.size;
                            for (const receiver of receivers) {
                                send(receiver, message)
                            }
                        }
                    }
                    break;
                case 'ping':
                    send(conn, {type: 'pong'})
            }
        }
    })
}

wss.on('connection', onconnection);

server.on('upgrade', (request, socket, head) => {
    const handleAuth = ws => {
        wss.emit('connection', ws, request)
    };
    wss.handleUpgrade(request, socket, head, handleAuth)
});

server.listen(port);

console.log('Signaling server running on localhost:', port);