import { io } from 'socket.io-client';
import { API_URL } from './api';

/** Cliente socket.io compartido (se conecta apenas se importa). */
export const socket = io(API_URL, {
  autoConnect: true,
  reconnection: true,
  reconnectionAttempts: Infinity,
  reconnectionDelay: 1000,
  reconnectionDelayMax: 5000,
});
