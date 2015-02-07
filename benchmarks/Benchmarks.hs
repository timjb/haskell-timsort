{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE FlexibleInstances #-}

module Main (main) where

import Criterion.Main
import qualified Data.Vector as V
import Test.QuickCheck
import Test.QuickCheck.Gen (Gen (..))
import Test.QuickCheck.Random (mkQCGen)
import Control.Monad.Primitive (PrimState)
import Data.Monoid (mappend)
import Control.Applicative ((<$>))
import Control.DeepSeq

import qualified Data.Vector.Algorithms.Intro as Intro
import qualified Data.Vector.Algorithms.Heap  as Heap
import qualified Data.Vector.Algorithms.Merge as Merge

import qualified Data.Vector.Algorithms.Tim   as Tim


type ListGen = Int -> Gen [Int]

lists :: [(String, ListGen)]
lists =
  [ ("sorted", \n -> return [1..n])
  , ("reverse sorted", \n -> return (reverse [1..n]))
  , ("random", \n -> vectorOf n arbitrary)
  , ("random (values 1,...,100)", \n -> vectorOf n (choose (1,100)))
  , ("monotonic runs", \n ->
      let maxRunLength = max 10 (n `div` 100)
          iter k | k <= 0 = return []
          iter k | otherwise = do
            runLength <- choose (1, min k maxRunLength)
            start <- choose (-10000,10000)
            step <- choose (-1000,1000)
            mappend (map (\i -> start + i*step) [1..runLength]) <$> iter (k - runLength)
      in iter n)
  ]

type SortAlgorithm = V.MVector (PrimState IO) Int -> IO ()

sortAlgorithms :: [(String, SortAlgorithm)]
sortAlgorithms =
  [ ("Tim sort", Tim.sort)
  , ("Heap sort", Heap.sort)
  , ("Merge sort", Merge.sort)
  , ("Intro sort", Intro.sort)
  ]

sizes :: [(String, Int)]
sizes =
  [ ("10", 10)
  , ("100", 100)
  , ("1k", 1000)
  , ("100k", 100000)
  ]

instance NFData (V.MVector s Int) where
  rnf a = a `seq` ()

benchmarks :: [Benchmark]
benchmarks = map (uncurry benchList) lists
  where seed = mkQCGen 420815
        benchList listName listGen =
          bgroup listName $ map (uncurry $ benchSize listGen) sizes
        benchSize listGen sizeName size =
          let vec = V.fromList $ unGen (listGen size) seed size
          in bgroup sizeName $ map (uncurry $ benchAlgo vec) sortAlgorithms
        benchAlgo vec algoName sortAlgo =
          env (V.thaw vec) $ \v -> bench algoName $ whnfIO (sortAlgo v)

main :: IO ()
main = defaultMain benchmarks