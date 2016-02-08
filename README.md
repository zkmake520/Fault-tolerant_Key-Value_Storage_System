# Gossip_Protocol
Gossip Protocol written in C++ for failure detection in P2P system. There are 3 layers:   <br/>
		Application layer: creates all members and start the each Node    <br/>
        P2P layer:  receive messages ,detect failures and send membership to neighboors<br/>
        Emulated Network: send and push message<br/>