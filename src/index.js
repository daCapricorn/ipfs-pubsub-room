'use strict'

const diff = require('hyperdiff')
const EventEmitter = require('events')
const timers = require('timers')
const clone = require('lodash.clonedeep')

const PROTOCOL = require('./protocol')
const Connection = require('./connection')
const encoding = require('./encoding')
const directConnection = require('./direct-connection-handler')
const libp2p = require('./libp2p')
const uuidv1 = require('uuid/v1');


const DEFAULT_OPTIONS = {
  pollInterval: 1000
}

module.exports = (ipfs, topic, options) => {
  return new PubSubRoom(ipfs, topic, options)
}

class PubSubRoom extends EventEmitter {
  constructor (ipfs, topic, options) {
    super()
    this._ipfs = ipfs
    this._topic = topic
    this._options = Object.assign({}, clone(DEFAULT_OPTIONS), clone(options))
    this._peers = []
    this._connections = {}

    this._handleDirectMessage = this._handleDirectMessage.bind(this)

    if (!this._ipfs.pubsub) {
      throw new Error('This IPFS node does not have pubsub.')
    }

    if (this._ipfs.isOnline()) {
      this._start()
    } else {
      this._ipfs.on('ready', this._start.bind(this))
    }

    this._ipfs.on('stop', this.leave.bind(this))
  }
  getMyPeerId(){
    return this._ipfs._peerInfo.id.toB58String();
  }
  getPeers () {
    return this._peers.slice(0)
  }

  hasPeer (peer) {
    return this._peers.indexOf(peer) >= 0
  }

  leave () {
    return new Promise((resolve, reject) => {
      timers.clearInterval(this._interval)
      Object.keys(this._connections).forEach((peer) => {
        this._connections[peer].stop()
      })
      directConnection.emitter.removeListener(this._topic, this._handleDirectMessage)
      this.once('stopped', () => resolve())
      this.emit('stopping')
    })
  }

  broadcast (_message) {
    let message = encoding(_message)

    this._ipfs.pubsub.publish(this._topic, message, (err) => {
      if (err) {
        this.emit('error', err)
      }
    })
  }

  sendTo (peer, message) {
    let conn = this._connections[peer]
    if (!conn) {
      conn = new Connection(peer, this._ipfs, this)
      conn.on('error', (err) => this.emit('error', err))
      this._connections[peer] = conn

      conn.once('disconnect', () => {
        console.log('sendTo disconnect cause peer left');
        delete this._connections[peer]
        this._peers = this._peers.filter((p) => p !== peer)
        this.emit('peer left', peer)
      })
    }

    // We should use the same sequence number generation as js-libp2p-floosub does:
    // const seqno = Buffer.from(utils.randomSeqno())

    // Until we figure out a good way to bring in the js-libp2p-floosub's randomSeqno
    // generator, let's use 0 as the sequence number for all private messages
    // const seqno = Buffer.from([0])
    const seqno = Buffer.from([0])

    const msg = {
      to: peer,
      from: this._ipfs._peerInfo.id.toB58String(),
      data: Buffer.from(message).toString('hex'),
      seqno: seqno.toString('hex'),
      topicIDs: [ this._topic ],
      topicCIDs: [ this._topic ]
    }

    conn.push(Buffer.from(JSON.stringify(msg)))
  }

  rpcRequest (peer, message, callback) {
    if(typeof callback != 'function'){
      return this.sendTo(peer, message);
    }
    let conn = this._connections[peer]
    if (!conn) {
      conn = new Connection(peer, this._ipfs, this)
      conn.on('error', (err) => this.emit('error', err))
      this._connections[peer] = conn

      conn.once('disconnect', () => {
        console.log('rpcRequest disconnect cause peer left');
        delete this._connections[peer]
        this._peers = this._peers.filter((p) => p !== peer)
        this.emit('peer left', peer)
      })
    }
    const guid = this._generateUUID();
    if(! this.callbackPool) this.callbackPool = {};
    const timer = setTimeout(() => {
      const callback = this.callbackPool && this.callbackPool[guid] && this.callbackPool[guid].callback;
      delete this.callbackPool[guid];
      if(typeof callback == 'function')
        callback(null, "timeout");
      else
        console.log('callback function not exists in rpcRequest callback');
      
    }, 30000);
    this.callbackPool[guid] = {timer, callback};
    // We should use the same sequence number generation as js-libp2p-floosub does:
    // const seqno = Buffer.from(utils.randomSeqno())

    // Until we figure out a good way to bring in the js-libp2p-floosub's randomSeqno
    // generator, let's use 0 as the sequence number for all private messages
    // const seqno = Buffer.from([0])
    const seqno = Buffer.from([0])

    const msg = {
      to: peer,
      verb:'request',
      guid,
      from: this._ipfs._peerInfo.id.toB58String(),
      data: message? Buffer.from(message).toString('hex'): '',
      seqno: seqno.toString('hex'),
      topicIDs: [ this._topic ],
      topicCIDs: [ this._topic ]
    }

    conn.push(Buffer.from(JSON.stringify(msg)))
  }

  rpcResponse (peer, message, guid, err) {
    let conn = this._connections[peer]
    if (!conn) {
      conn = new Connection(peer, this._ipfs, this)
      conn.on('error', (err) => this.emit('error', err))
      this._connections[peer] = conn

      conn.once('disconnect', () => {
        console.log('rpcResponse disconnect cause peer left');
        delete this._connections[peer]
        this._peers = this._peers.filter((p) => p !== peer)
        this.emit('peer left', peer)
      })
    }
    
    // We should use the same sequence number generation as js-libp2p-floosub does:
    // const seqno = Buffer.from(utils.randomSeqno())

    // Until we figure out a good way to bring in the js-libp2p-floosub's randomSeqno
    // generator, let's use 0 as the sequence number for all private messages
    // const seqno = Buffer.from([0])
    const seqno = Buffer.from([0])

    const msg = {
      to: peer,
      verb:'response',
      guid,
      from: this._ipfs._peerInfo.id.toB58String(),
      data: message? Buffer.from(message).toString('hex'): '',
      seqno: seqno.toString('hex'),
      topicIDs: [ this._topic ],
      topicCIDs: [ this._topic ]
    }
    if(err) msg.err = err;
    conn.push(Buffer.from(JSON.stringify(msg)))
  }

  rpcResponseWithNewRequest (peer, message, guid, newCallback, err) {
    let conn = this._connections[peer]
    if (!conn) {
      conn = new Connection(peer, this._ipfs, this)
      conn.on('error', (err) => this.emit('error', err))
      this._connections[peer] = conn

      conn.once('disconnect', () => {
        console.log('rpcResponseWithNewRequest disconnect cause peer left');
        delete this._connections[peer]
        this._peers = this._peers.filter((p) => p !== peer)
        this.emit('peer left', peer)
      })
    }
    
    const guidForNewRequest = this._generateUUID();
    if(! this.callbackPool) this.callbackPool = {};
    const timer = setTimeout(() => {
      const callback = this.callbackPool && this.callbackPool[guidForNewRequest] && this.callbackPool[guidForNewRequest].callback;
      delete this.callbackPool[guidForNewRequest];
      if(typeof callback == 'function')
        callback(null, "timeout");
      
      
    }, 30000);
    this.callbackPool[guidForNewRequest] = {timer, callback: newCallback};

    // We should use the same sequence number generation as js-libp2p-floosub does:
    // const seqno = Buffer.from(utils.randomSeqno())

    // Until we figure out a good way to bring in the js-libp2p-floosub's randomSeqno
    // generator, let's use 0 as the sequence number for all private messages
    // const seqno = Buffer.from([0])
    const seqno = Buffer.from([0])

    const msg = {
      to: peer,
      verb:'responseWithNewRequest',
      guid,
      guidForNewRequest,
      from: this._ipfs._peerInfo.id.toB58String(),
      data: message? Buffer.from(message).toString('hex'): '',
      seqno: seqno.toString('hex'),
      topicIDs: [ this._topic ],
      topicCIDs: [ this._topic ]
    }
    if(err) msg.err = err;
    conn.push(Buffer.from(JSON.stringify(msg)))
  }

  _handleDirectMessage (message) {
    if (message.to === this._ipfs._peerInfo.id.toB58String()) {
      const m = Object.assign({}, message)
      if(m.verb == 'request'){
        delete m.to
        this.emit('rpcDirect', m) //let the event listener to handle this message and call rpcResponse() to send response back
      }else if(m.verb == 'response'){
        if(! m.guid){
          console.error('rpc repsonse message should always have a guid', m.guid);
          
        }
        else if(! this.callbackPool){
          console.error('rpc response received, but callbackPool is empty');
        }
        else if(! this.callbackPool[m.guid]){
          console.error('rpc response received, callbackPool exists, but cannot find m.guid', m.guid);
        }
        else{
          const {timer, callback} = this.callbackPool[m.guid];
          if(typeof callback == 'function'){
            clearTimeout(this.callbackPool[m.guid].timer);
            const tryParseJson = (s)=>{
              try{
                return JSON.parse(s);
              }
              catch(e){
                return undefined;
              }
            }
            const responseObj = tryParseJson(m.data.toString());
            delete this.callbackPool[m.guid];
            if(responseObj){
              try{
                callback(responseObj, null);
              }
              catch(e){
                console.error('Unhandled exception inside rpc callback result handler. The responseObj and exception are:', responseObj, e);
              }
            }else{
              try{
                callback(null, m.err);
              }
              catch(e){
                console.error('unhandled exception inside rpc callback error handler. The m.err and exception are:',  m.err, e);
              }
            }
            
            return;
          
          }else{
            return console.log('calblack is not a function', callback);
          }
        }
      }else if(m.verb == 'responseWithNewRequest'){
        if(m.guid && this.callbackPool && this.callbackPool[m.guid]){
          const {timer, callback} = this.callbackPool[m.guid];
          if(typeof callback == 'function'){
            clearTimeout(this.callbackPool[m.guid].timer);
            const tryParseJson = (s)=>{
              try{
                return JSON.parse(s);
              }
              catch(e){
                return undefined;
              }
            }
            const responseObj = tryParseJson(m.data.toString());
            delete this.callbackPool[m.guid];
            if(responseObj){
              callback(responseObj, null, m.guidForNewRequest);
            }else{
              callback(null, m.err, m.guidForNewRequest);
            }
            
            
            return;
          
          }else{
            return console.log('calblack is not a function', callback);
          }
        }else{
          //possible timeout. nothing we can do, just drop this message
          //console.log('possible timeout. nothing we can do, just drop this message');
          return;
        }
      }
      else{
        //a standard message. we need to be backward compatible. so just emit a message
        delete m.to
        this.emit('message', m)
      }
    }
  }

  _start () {
    this._interval = timers.setInterval(
      this._pollPeers.bind(this),
      this._options.pollInterval)

    const listener = this._onMessage.bind(this)
    this._ipfs.pubsub.subscribe(this._topic, listener, {}, (err) => {
      if (err) {
        this.emit('error', err)
      } else {
        this.emit('subscribed', this._topic)
      }
    })

    this.once('stopping', () => {
      this._ipfs.pubsub.unsubscribe(this._topic, listener, (err) => {
        if (err) {
          this.emit('error', err)
        } else {
          this.emit('stopped')
        }
      })
    })

    libp2p(this._ipfs).handle(PROTOCOL, directConnection.handler)

    directConnection.emitter.on(this._topic, this._handleDirectMessage)
  }

  _pollPeers () {
    this._ipfs.pubsub.peers(this._topic, (err, _newPeers) => {
      if (err) {
        this.emit('error', err)
        return // early
      }

      const newPeers = _newPeers.sort()

      if (this._emitChanges(newPeers)) {
        this._peers = newPeers
      }
    })
  }

  _emitChanges (newPeers) {
    const differences = diff(this._peers, newPeers)

    differences.added.forEach((addedPeer) => this.emit('peer joined', addedPeer))
    differences.removed.forEach((removedPeer) => this.emit('peer left', removedPeer))

    return differences.added.length > 0 || differences.removed.length > 0
  }

  _onMessage (message) {
    this.emit('message', message)
  }



  _generateUUID() { // Public Domain/MIT
    return uuidv1();
  }
}
