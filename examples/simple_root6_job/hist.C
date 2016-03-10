{
  TH1F h("h", "", 10, 0, 1);
  h.FillRandom("gaus", 10000);
  TCanvas c("c", "", 800, 800);
  h.Draw();
  c.SaveAs("hist.pdf");
}