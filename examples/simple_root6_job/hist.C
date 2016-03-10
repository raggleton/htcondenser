{
  // Make a hist, print to file
  TH1F h("h", "", 10, 0, 1);
  h.FillRandom("gaus", 10000);
  TCanvas c("c", "", 800, 800);
  h.Draw();
  c.SaveAs("hist.pdf");

  // Save stuff to a TTree in a TFile
  float pt(0.), eta(0.);

  TFile *f = TFile::Open("simple_tree.root","RECREATE");
  TTree *t = new TTree("tree","Simple TTree");
  t->Branch("pt", &pt);
  t->Branch("eta", &eta);

  for(int i = 0; i < 100; ++i) {
    pt = gRandom->Rndm(1)*150;
    eta = (gRandom->Rndm(1)*10.) - 5.;
    t->Fill();
  }
  f->Write();
  f->Close();
}