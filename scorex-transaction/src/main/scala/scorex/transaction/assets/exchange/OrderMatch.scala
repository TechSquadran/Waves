package scorex.transaction.assets.exchange

import scala.util.Try

import com.google.common.primitives.{Ints, Longs}
import play.api.libs.json.{JsObject, Json}
import scorex.account.{PrivateKeyAccount, PublicKeyAccount}
import scorex.crypto.EllipticCurveImpl
import scorex.crypto.encode.Base58
import scorex.crypto.hash.FastCryptographicHash
import scorex.serialization.{BytesSerializable, Deser}
import scorex.transaction.TypedTransaction.TransactionType
import scorex.transaction._
import scorex.transaction.assets.exchange.Validation.BooleanOperators


/**
  * Transaction with matched orders generated by Matcher service
  */
@SerialVersionUID(745449986166135919L)
case class OrderMatch(buyOrder: Order, sellOrder: Order, price: Long, amount: Long, buyMatcherFee: Long,
                      sellMatcherFee: Long, fee: Long, timestamp: Long, signature: Array[Byte])
  extends SignedTransaction with BytesSerializable {

  override val transactionType: TransactionType.Value = TransactionType.OrderMatchTransaction

  override val typeOrdering = -1

  override lazy val id: Array[Byte] = FastCryptographicHash(toSign)

  lazy val idStr: String = Base58.encode(id)

  override val assetFee: (Option[AssetId], Long) = (None, fee)

  override val sender: PublicKeyAccount = buyOrder.matcher

  def isValid(previousMatches: Set[OrderMatch]): Validation =  {
    lazy val buyTransactions = previousMatches.filter { om =>
      om.buyOrder.id sameElements buyOrder.id
    }
    lazy val sellTransactions = previousMatches.filter { om =>
      om.sellOrder.id sameElements sellOrder.id
    }

    lazy val buyTotal = buyTransactions.foldLeft(0L)(_ + _.amount) + amount
    lazy val sellTotal = sellTransactions.foldLeft(0L)(_ + _.amount) + amount

    lazy val buyFeeTotal = buyTransactions.map(_.buyMatcherFee).sum + buyMatcherFee
    lazy val sellFeeTotal = sellTransactions.map(_.sellMatcherFee).sum + sellMatcherFee

    lazy val isSameAssets = {
      buyOrder.assetPair == sellOrder.assetPair
    }

    lazy val isSameMatchers = {
      buyOrder.matcher == sellOrder.matcher
    }

    lazy val priceIsValid: Boolean = price <= buyOrder.price && price >= sellOrder.price

    lazy val amountIsValid: Boolean = {
      val b = buyTotal <= buyOrder.amount
      val s = sellTotal <= sellOrder.amount
      b && s
    }

    def isFeeValid(fee: Long, feeTotal: Long, amountTotal: Long, maxfee: Long, maxAmount: Long): Boolean = {
      fee > 0 &&
        feeTotal <= BigInt(maxfee) * BigInt(amountTotal) / BigInt(maxAmount)
    }

    (fee > 0) :| "fee should be > 0" &&
      (amount > 0) :| "amount should be > 0" &&
      (price > 0) :| "price should be > 0" &&
      (price < Order.MaxAmount) :| "price too large" &&
      (amount < Order.MaxAmount) :| "amount too large" &&
      (sellMatcherFee < Order.MaxAmount) :| "sellMatcherFee too large" &&
      (buyMatcherFee < Order.MaxAmount) :| "buyMatcherFee too large" &&
      (fee < Order.MaxAmount) :| "fee too large" &&
      (buyOrder.orderType == OrderType.BUY) :| "buyOrder should has OrderType.BUY" &&
      (sellOrder.orderType == OrderType.SELL) :| "sellOrder should has OrderType.SELL" &&
      isSameMatchers :| "Both orders should have same Matcher" &&
      isSameAssets :| "Both orders should have same AssetPair" &&
      ("buyOrder" |: buyOrder.isValid(timestamp)) &&
      ("sellOrder" |: sellOrder.isValid(timestamp)) &&
      priceIsValid :| "price should be valid" &&
      amountIsValid :| "amount should be valid" &&
      isFeeValid(buyMatcherFee, buyFeeTotal, buyTotal, buyOrder.matcherFee, buyOrder.amount) :|
        "buyMatcherFee should be valid" &&
      isFeeValid(sellMatcherFee, sellFeeTotal, sellTotal, sellOrder.matcherFee, sellOrder.amount) :|
        "sellMatcherFee should be valid" &&
      signatureValid :|  "matcherSignatureIsValid should be valid"
  }

  lazy val toSign: Array[Byte] = Array(transactionType.id.toByte) ++
    Ints.toByteArray(buyOrder.bytes.length) ++ Ints.toByteArray(sellOrder.bytes.length) ++
    buyOrder.bytes ++ sellOrder.bytes ++ Longs.toByteArray(price) ++ Longs.toByteArray(amount) ++
    Longs.toByteArray(buyMatcherFee) ++ Longs.toByteArray(sellMatcherFee) ++ Longs.toByteArray(fee) ++
    Longs.toByteArray(timestamp)

  override def bytes: Array[Byte] = toSign ++ signature

  override def json: JsObject = Json.obj(
    "order1" -> buyOrder.json,
    "order2" -> sellOrder.json,
    "price" -> price,
    "amount" -> amount,
    "buyMatcherFee" -> buyMatcherFee,
    "sellMatcherFee" -> sellMatcherFee,
    "fee" -> fee,
    "timestamp" -> timestamp,
    "signature" -> Base58.encode(signature)
  )

  override def balanceChanges(): Seq[BalanceChange] = {

    val matcherChange = Seq(BalanceChange(AssetAcc(buyOrder.matcher, None), buyMatcherFee + sellMatcherFee - fee))
    val buyFeeChange = Seq(BalanceChange(AssetAcc(buyOrder.sender, None), -buyMatcherFee))
    val sellFeeChange = Seq(BalanceChange(AssetAcc(sellOrder.sender, None), -sellMatcherFee))

    val exchange = Seq(
      (buyOrder.sender, (buyOrder.spendAssetId, -amount)),
      (buyOrder.sender, (buyOrder.receiveAssetId, (BigInt(amount) * Order.PriceConstant / price).longValue())),
      (sellOrder.sender, (sellOrder.receiveAssetId, amount)),
      (sellOrder.sender, (sellOrder.spendAssetId, -(BigInt(amount) * Order.PriceConstant / price).longValue()))
    )

    buyFeeChange ++ sellFeeChange ++ matcherChange ++
      exchange.map(c => BalanceChange(AssetAcc(c._1, c._2._1), c._2._2))
  }

  override def validate: ValidationResult.Value = ???
}

object OrderMatch extends Deser[OrderMatch] {
  override def parseBytes(bytes: Array[Byte]): Try[OrderMatch] = Try {
    require(bytes.head == TransactionType.OrderMatchTransaction.id)
    parseTail(bytes.tail).get
  }

  def parseTail(bytes: Array[Byte]): Try[OrderMatch] = Try {
    import EllipticCurveImpl._
    var from = 0
    val o1Size = Ints.fromByteArray(bytes.slice(from, from + 4)); from += 4
    val o2Size = Ints.fromByteArray(bytes.slice(from, from + 4)); from += 4
    val o1 = Order.parseBytes(bytes.slice(from, from + o1Size)).get; from += o1Size
    val o2 = Order.parseBytes(bytes.slice(from, from + o2Size)).get; from += o2Size
    val price = Longs.fromByteArray(bytes.slice(from, from + 8)); from += 8
    val amount = Longs.fromByteArray(bytes.slice(from, from + 8)); from += 8
    val buyMatcherFee = Longs.fromByteArray(bytes.slice(from, from + 8)); from += 8
    val sellMatcherFee = Longs.fromByteArray(bytes.slice(from, from + 8)); from += 8
    val fee = Longs.fromByteArray(bytes.slice(from, from + 8)); from += 8
    val timestamp = Longs.fromByteArray(bytes.slice(from, from + 8)); from += 8
    val signature = bytes.slice(from, from + SignatureLength); from += SignatureLength
    OrderMatch(o1, o2, price, amount, buyMatcherFee, sellMatcherFee, fee, timestamp, signature)
  }

  def create(matcher: PrivateKeyAccount, buyOrder: Order, sellOrder: Order, price: Long, amount: Long,
             buyMatcherFee: Long, sellMatcherFee: Long, fee: Long, timestamp: Long): OrderMatch = {
    val unsigned = OrderMatch(buyOrder, sellOrder, price, amount, buyMatcherFee, sellMatcherFee, fee, timestamp, Array())
    val sig = EllipticCurveImpl.sign(matcher, unsigned.toSign)
    unsigned.copy(signature = sig)
  }

  def create(matcher: PrivateKeyAccount, buyOrder: Order, sellOrder: Order, price: Long, amount: Long,
             fee: Long, timestamp: Long): OrderMatch = {
    val buyMatcherFee = BigInt(buyOrder.matcherFee) * amount / buyOrder.amount
    val sellMatcherFee = BigInt(sellOrder.matcherFee) * amount / sellOrder.amount
    val unsigned = OrderMatch(buyOrder, sellOrder, price, amount, buyMatcherFee.toLong,
      sellMatcherFee.toLong, fee, timestamp, Array())
    val sig = EllipticCurveImpl.sign(matcher, unsigned.toSign)
    unsigned.copy(signature = sig)
  }
}
