from types import SimpleNamespace

import torch
from torch.optim.adam import Adam
from torch_geometric.nn.models import Node2Vec

torch.set_num_threads(16)
HP = SimpleNamespace(
    latent_dim  = 64,
    walk_len    = 10,
    num_walks   = 200,
    lr          = 0.01,
    epochs      = 500,
    batch_size  = 512
)


def train(g):
    model = Node2Vec(
        g.edge_index, HP.latent_dim, HP.walk_len,
        context_size=HP.walk_len, num_nodes=g.x.size(0)
    )
    model.train()
    opt = Adam(model.parameters(), lr=HP.lr)

    for e in range(HP.epochs):
        batches = torch.randperm(g.x.size(0))
        batches = batches.split(HP.batch_size)

        for i,b in enumerate(batches):
            opt.zero_grad()
            p,n = model.sample(b)
            loss = model.loss(p,n)
            loss.backward()
            opt.step()

        print(f'[{e}] Loss: {loss.item():0.4f}')

    model.eval()
    with torch.no_grad():
        zs = model.forward()
    torch.save(zs, f'n2v-{HP.walk_len}-{HP.latent_dim}.pt')

g = torch.load('kg.pt', weights_only=False)
train(g)